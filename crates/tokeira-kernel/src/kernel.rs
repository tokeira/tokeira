//! Pure state-machine kernel for workflow execution.
//!
//! This module is the single source of truth for all workflow state
//! transitions. It enforces a strict contract: one writer at a time,
//! zero I/O, and deterministic outputs for any given (state, command)
//! pair. That contract is what makes the kernel safe to test with
//! golden-file snapshots and property-based fuzzing, and what lets
//! the runtime layer treat it as an opaque, infallible oracle.

use std::collections::BTreeMap;

use smallvec::SmallVec;
use thiserror::Error;
use time::{Duration, OffsetDateTime};
use tokeira_types::{
    BuildId, DeploymentId, ExecutionStatus, LogicalTaskSeq, NamespaceId, Payloads, QueueKey, RunId,
    RunKey, StickyAffinity, TaskQueueName, TransitionSeq, WorkerIdentity, WorkflowId,
};

use crate::{
    command::{
        ActivityResolvedRequest, CallbackAttemptOutcome, CancelRequest, ChildResolution,
        ChildResolvedRequest, ChildStartConfirmedRequest, ChildStartResult, Command,
        CompletionCallbackAttemptedRequest, ContinueAsNewInitiator, CronContinuation,
        ExternalCancelResolvedRequest, ExternalCancelResult, ExternalSignalResolvedRequest,
        ExternalSignalResult, FieldChange, NexusOperationResolvedRequest,
        NexusOperationRetryRequest, NexusResolution, PauseActivityRequest, PauseWorkflowRequest,
        ResetActivityRequest, ResetRequest, ResetStickyRequest, RetryContinuation, RetryState,
        ScheduleQueryTaskRequest, SignalRequest, SignalWithStartRequest, StartAndUpdateRequest,
        StartRequest, StartWorkflowTaskRequest, TerminateOnWorkflowTaskFailedRequest,
        TerminateRequest, TimerDueRequest, UnpauseActivityRequest, UnpauseWorkflowRequest,
        UpdateActivityOptionsRequest, UpdateExecutionOptionsRequest, UpdateProtocolBody,
        UpdateRequest, WorkflowCommand, WorkflowExecutionTimedOutRequest,
        WorkflowStartDelayElapsedRequest, WorkflowTaskCompletedRequest, WorkflowTaskFailedCause,
        WorkflowTaskFailedRequest, WorkflowTaskTimedOutRequest, WorkflowTaskTimeoutType,
    },
    event::{ActivityResolution, HistoryEvent, HistoryEventKind},
    state::{
        ActivityPauseInfo, ActivityState, ChildWorkflowState,
        EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
        LoadedRun, ParentClosePolicy, PauseInfo, PendingExternalCancel, PendingExternalSignal,
        PendingNexusOperation, PendingUpdate, PendingWorkflowTask, RequestIdInfo, TimerState,
        VersioningOverride, WorkflowState, WorkflowVersioningInfo,
    },
    transition::{
        ActivityOp, CallbackCompletionOutcome, DispatchOp, ProjectionOp, RequestDedupeOp, TimerOp,
        Transition,
    },
};

/// Derive the completion-callback outcome from a run's terminal event.
///
/// Mirrors v1.31.0's `GetNexusCompletion`, which switches on the workflow
/// completion event type to build the Nexus completion
/// (`service/history/workflow/mutable_state_impl.go @ v1.31.0`): completed → the
/// first result payload; failed → the failure; canceled → cancellation details;
/// terminated/timed-out → a synthesized failure (synthesis happens in the runtime,
/// so the kernel only marks the variant); continued-as-new has no upstream mapping
/// (v1.31.0 returns an internal error) and is forwarded as its own variant so the
/// runtime can fail the operation rather than hang the caller. A non-terminal event
/// yields `None` (the callback is not dispatched).
///
/// Public so the runtime's completion-callback retry scanner re-derives a re-fired
/// callback's outcome from the *same* terminal event (read back from history) and the
/// *same* function the close path used — keeping a retry byte-identical to the first
/// attempt (incl. canceled details) without duplicating this mapping or persisting it.
pub fn callback_completion_outcome(kind: &HistoryEventKind) -> Option<CallbackCompletionOutcome> {
    match kind {
        HistoryEventKind::WorkflowExecutionCompleted { result, .. } => {
            Some(CallbackCompletionOutcome::Success {
                result: result.0.first().cloned(),
            })
        }
        HistoryEventKind::WorkflowExecutionFailed { failure, .. } => {
            Some(CallbackCompletionOutcome::Failed {
                failure: failure.clone(),
            })
        }
        HistoryEventKind::WorkflowExecutionCanceled { details, .. } => {
            Some(CallbackCompletionOutcome::Canceled {
                details: details.clone(),
            })
        }
        HistoryEventKind::WorkflowExecutionTerminated { .. } => {
            Some(CallbackCompletionOutcome::Terminated)
        }
        HistoryEventKind::WorkflowExecutionTimedOut { .. } => {
            Some(CallbackCompletionOutcome::TimedOut)
        }
        HistoryEventKind::WorkflowExecutionContinuedAsNew { .. } => {
            Some(CallbackCompletionOutcome::ContinuedAsNew)
        }
        _ => None,
    }
}

fn stamp_callback_registration_times(
    callbacks: &mut [crate::state::CompletionCallback],
    registration_time: OffsetDateTime,
) {
    for callback in callbacks {
        if callback.registration_time.is_none() {
            callback.registration_time = Some(registration_time);
        }
    }
}

pub const WORKFLOW_START_DELAY_TIMER_ID: &str = "__tokeira_workflow_start_delay";

fn positive_start_delay(delay: Option<time::Duration>) -> Option<time::Duration> {
    delay.filter(|delay| *delay > time::Duration::ZERO)
}

/// Pure transition engine.
///
/// Given a loaded run state and one validated command, the
/// kernel computes the exact transition that should happen
/// next — with no hidden I/O and no side effects.
///
/// See `docs/architecture/020-kernel.md` for the full design
/// rationale.
pub trait Kernel {
    /// Apply a single command to a loaded run and return the
    /// authoritative transition, or reject the command.
    fn apply(&self, loaded: LoadedRun, command: Command) -> Result<Transition, Reject>;
}

/// Default kernel implementation.
///
/// Stateless — all inputs arrive via `apply` arguments and
/// all outputs leave via the returned `Transition`. This
/// makes the kernel trivially testable with golden-file and
/// property-based tests.
#[derive(Default)]
pub struct BasicKernel;

/// Additional durable identity/config needed to replay a history prefix.
///
/// History is authoritative for semantic state transitions, but some envelope
/// fields live outside individual history events and must be supplied by the
/// caller when reconstructing state.
#[derive(Clone, Debug, PartialEq)]
pub struct ReplayContext {
    pub run_key: RunKey,
    pub namespace_id: NamespaceId,
    pub workflow_id: WorkflowId,
    pub run_id: RunId,
    pub deployment: Option<DeploymentId>,
    pub build_id: Option<BuildId>,
    pub parent_run_key: Option<RunKey>,
    pub parent_workflow_id: Option<WorkflowId>,
    pub first_run_started_at: Option<OffsetDateTime>,
}

impl Kernel for BasicKernel {
    fn apply(&self, loaded: LoadedRun, command: Command) -> Result<Transition, Reject> {
        match command {
            Command::Start(req) => self.apply_start(loaded, req),
            Command::SignalWithStart(req) => self.apply_signal_with_start(loaded, req),
            Command::StartAndUpdate(req) => self.apply_start_and_update(loaded, req),
            Command::Update(req) => self.apply_update(loaded, req),
            Command::Signal(req) => self.apply_signal(loaded, req),
            Command::Cancel(req) => self.apply_cancel(loaded, req),
            Command::Terminate(req) => self.apply_terminate(loaded, req),
            Command::Reset(req) => self.apply_reset(loaded, req),
            Command::PauseWorkflow(req) => self.apply_pause_workflow(loaded, req),
            Command::UnpauseWorkflow(req) => self.apply_unpause_workflow(loaded, req),
            Command::UpdateActivityOptions(req) => self.apply_update_activity_options(loaded, req),
            Command::PauseActivity(req) => self.apply_pause_activity(loaded, req),
            Command::UnpauseActivity(req) => self.apply_unpause_activity(loaded, req),
            Command::ResetActivity(req) => self.apply_reset_activity(loaded, req),
            Command::UpdateExecutionOptions(req) => {
                self.apply_update_execution_options(loaded, req)
            }
            Command::WorkflowExecutionTimedOut(req) => {
                self.apply_workflow_execution_timed_out(loaded, req)
            }
            Command::WorkflowTaskStarted(req) => self.apply_workflow_task_started(loaded, req),
            Command::StartDeploymentTransition(req) => {
                self.apply_start_deployment_transition(loaded, req)
            }
            Command::WorkflowTaskCompleted(req) => {
                self.apply_workflow_task_completed(loaded, req, None, None)
            }
            Command::WorkflowTaskCompletedWithCron {
                request,
                cron_continuation,
            } => self.apply_workflow_task_completed(loaded, request, Some(cron_continuation), None),
            Command::WorkflowTaskCompletedWithRetry {
                request,
                retry_continuation,
            } => {
                self.apply_workflow_task_completed(loaded, request, None, Some(retry_continuation))
            }
            Command::WorkflowTaskFailed(req) => self.apply_workflow_task_failed(loaded, req),
            Command::TerminateOnWorkflowTaskFailed(req) => {
                self.apply_terminate_on_workflow_task_failed(loaded, req)
            }
            Command::WorkflowTaskTimedOut(req) => self.apply_workflow_task_timed_out(loaded, req),
            Command::ResetSticky(req) => self.apply_reset_sticky(loaded, req),
            Command::ActivityResolved(req) => self.apply_activity_resolved(loaded, req),
            Command::ChildStartConfirmed(req) => self.apply_child_start_confirmed(loaded, req),
            Command::ChildResolved(req) => self.apply_child_resolved(loaded, req),
            Command::ExternalSignalResolved(req) => {
                self.apply_external_signal_resolved(loaded, req)
            }
            Command::ExternalCancelResolved(req) => {
                self.apply_external_cancel_resolved(loaded, req)
            }
            Command::NexusOperationResolved(req) => {
                self.apply_nexus_operation_resolved(loaded, req)
            }
            Command::NexusOperationRetry(req) => self.apply_nexus_operation_retry(loaded, req),
            Command::TimerDue(req) => self.apply_timer_due(loaded, req),
            Command::WorkflowStartDelayElapsed(req) => {
                self.apply_workflow_start_delay_elapsed(loaded, req)
            }
            Command::ScheduleQueryTask(req) => self.apply_schedule_query_task(loaded, req),
            Command::CompletionCallbackAttempted(req) => {
                self.apply_completion_callback_attempted(loaded, req)
            }
        }
    }
}

impl BasicKernel {
    /// Reconstruct mutable workflow state from a committed history prefix.
    ///
    /// This is intentionally narrower than a full replay engine: it rebuilds
    /// durable kernel state from already-recorded history events, plus the
    /// non-historical envelope fields supplied by [`ReplayContext`].
    pub fn replay_history_prefix(
        &self,
        ctx: ReplayContext,
        events: &[HistoryEvent],
    ) -> Result<WorkflowState, Reject> {
        let first = events.first().ok_or(Reject::InvalidReplayHistory)?;
        let HistoryEventKind::WorkflowExecutionStarted {
            workflow_type,
            task_queue,
            memo,
            search_attributes,
            retry_policy,
            attempt,
            first_execution_run_id,
            original_execution_run_id,
            workflow_execution_timeout,
            workflow_run_timeout,
            workflow_task_timeout,
            parent_run_id,
            parent_namespace_id,
            parent_initiated_event_id,
            root_workflow_id,
            root_run_id,
            last_completion_result,
            workflow_start_delay,
            completion_callbacks,
            user_metadata,
            links,
            priority,
            versioning_info,
            worker_deployment_name,
            request_id,
            ..
        } = &first.kind
        else {
            return Err(Reject::InvalidReplayHistory);
        };

        let canonical_root_workflow_id = root_workflow_id
            .clone()
            .unwrap_or_else(|| ctx.workflow_id.clone());
        let canonical_root_run_id = root_run_id.unwrap_or(ctx.run_id);
        let mut state = WorkflowState {
            run_key: ctx.run_key,
            namespace_id: ctx.namespace_id,
            workflow_id: ctx.workflow_id,
            run_id: ctx.run_id,
            workflow_type: workflow_type.clone(),
            task_queue: task_queue.clone(),
            deployment: ctx.deployment,
            build_id: ctx.build_id,
            versioning_info: versioning_info.clone(),
            worker_deployment_name: worker_deployment_name.clone(),
            status: ExecutionStatus::Running,
            transition_seq: TransitionSeq::ZERO,
            last_event_id: first.event_id,
            next_workflow_task_seq: LogicalTaskSeq::ONE,
            pending_workflow_task: None,
            previous_started_event_id: 0,
            workflow_task_attempt: 1,
            sticky: None,
            pause_info: None,
            cancel_requested: false,
            wft_stamp: 0,
            memo: memo.clone(),
            search_attributes: search_attributes.clone(),
            workflow_execution_timeout: *workflow_execution_timeout,
            workflow_run_timeout: *workflow_run_timeout,
            workflow_task_timeout: *workflow_task_timeout,
            retry_policy: retry_policy.clone(),
            attempt: *attempt,
            first_execution_run_id: *first_execution_run_id,
            original_execution_run_id: *original_execution_run_id,
            parent_run_key: ctx.parent_run_key,
            parent_workflow_id: ctx.parent_workflow_id,
            parent_run_id: *parent_run_id,
            parent_namespace_id: *parent_namespace_id,
            parent_initiated_event_id: *parent_initiated_event_id,
            root_workflow_id: Some(canonical_root_workflow_id),
            root_run_id: Some(canonical_root_run_id),
            last_completion_result: last_completion_result.clone(),
            activities: BTreeMap::new(),
            timers: BTreeMap::new(),
            children: BTreeMap::new(),
            pending_external_signals: BTreeMap::new(),
            pending_external_cancels: BTreeMap::new(),
            pending_updates: BTreeMap::new(),
            admitted_updates: std::collections::HashSet::new(),
            pending_nexus_operations: BTreeMap::new(),
            completion_callbacks: completion_callbacks.clone(),
            user_metadata: user_metadata.clone(),
            links: links.clone(),
            workflow_start_delay: *workflow_start_delay,
            priority: priority.clone(),
            started_at: first.happened_at,
            first_run_started_at: ctx.first_run_started_at,
            closed_at: None,
            close_result: None,
            close_failure: None,
            request_id_infos: BTreeMap::new(),
            buffered_events: Vec::new(),
        };
        // Rebuild the start request id → WorkflowExecutionStarted mapping on cold
        // replay (the hot-state path records it in `apply_start`); kept in sync so
        // a reconstructed run reports the same request_id_infos.
        state.request_id_infos.insert(
            request_id.clone(),
            RequestIdInfo {
                event_id: first.event_id,
                event_type: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
                buffered: false,
            },
        );
        if let Some(delay) = positive_start_delay(*workflow_start_delay) {
            let timer = TimerState {
                timer_id: WORKFLOW_START_DELAY_TIMER_ID.to_string(),
                started_event_id: 0,
                fire_at: first.happened_at + delay,
            };
            state.timers.insert(timer.timer_id.clone(), timer);
        }

        for event in events {
            state.last_event_id = event.event_id;
            self.apply_replayed_event(&mut state, event);
        }

        Ok(state)
    }

    /// Bootstrap a brand-new workflow run from a `StartWorkflowExecution` request.
    /// Requires `LoadedRun::Absent` — the run must not already exist in storage.
    fn apply_start(&self, loaded: LoadedRun, req: StartRequest) -> Result<Transition, Reject> {
        if !matches!(loaded, LoadedRun::Absent) {
            return Err(Reject::RunAlreadyExists);
        }
        // Captured before the started-event emit below moves the request id.
        let start_request_id = req.request.request_id.0.clone();

        let canonical_root_workflow_id = req
            .root_workflow_id
            .clone()
            .unwrap_or_else(|| req.workflow_id.clone());
        let canonical_root_run_id = req.root_run_id.unwrap_or(req.run_id);
        let event_root_workflow_id = req.root_workflow_id.clone();
        let event_root_run_id = req.root_run_id;
        let initial_versioning_info = initial_versioning_info(req.versioning_override.clone());
        let mut completion_callbacks = req.completion_callbacks.clone();
        stamp_callback_registration_times(&mut completion_callbacks, req.now);
        let initial = WorkflowState {
            run_key: req.run_key,
            namespace_id: req.namespace_id,
            workflow_id: req.workflow_id,
            run_id: req.run_id,
            workflow_type: req.workflow_type.clone(),
            task_queue: req.task_queue.clone(),
            deployment: req.deployment.clone(),
            build_id: req.build_id.clone(),
            versioning_info: initial_versioning_info.clone(),
            worker_deployment_name: None,
            status: ExecutionStatus::Running,
            transition_seq: TransitionSeq::ZERO,
            last_event_id: 0,
            next_workflow_task_seq: LogicalTaskSeq::ONE,
            pending_workflow_task: None,
            previous_started_event_id: 0,
            workflow_task_attempt: 1,
            sticky: None,
            pause_info: None,
            cancel_requested: false,
            wft_stamp: 0,
            memo: req.memo.clone(),
            search_attributes: req.search_attributes.clone(),
            workflow_execution_timeout: req.workflow_execution_timeout,
            workflow_run_timeout: req.workflow_run_timeout,
            workflow_task_timeout: req.workflow_task_timeout,
            retry_policy: req.retry_policy.clone(),
            attempt: req.attempt,
            first_execution_run_id: req.first_execution_run_id,
            original_execution_run_id: req.original_execution_run_id.or(Some(req.run_id)),
            parent_run_key: req.parent_run_key,
            parent_workflow_id: req.parent_workflow_id.clone(),
            parent_run_id: req.parent_run_id,
            parent_namespace_id: req.parent_namespace_id,
            parent_initiated_event_id: req.parent_initiated_event_id,
            root_workflow_id: Some(canonical_root_workflow_id),
            root_run_id: Some(canonical_root_run_id),
            last_completion_result: req.last_completion_result.clone(),
            activities: BTreeMap::new(),
            timers: BTreeMap::new(),
            children: BTreeMap::new(),
            pending_external_signals: BTreeMap::new(),
            pending_external_cancels: BTreeMap::new(),
            pending_updates: BTreeMap::new(),
            admitted_updates: std::collections::HashSet::new(),
            pending_nexus_operations: BTreeMap::new(),
            completion_callbacks: completion_callbacks.clone(),
            user_metadata: req.user_metadata.clone(),
            links: req.links.clone(),
            workflow_start_delay: req.workflow_start_delay,
            priority: req.priority.clone(),
            started_at: req.now,
            first_run_started_at: req.first_run_started_at,
            closed_at: None,
            close_result: None,
            close_failure: None,
            request_id_infos: BTreeMap::new(),
            buffered_events: Vec::new(),
        };

        let mut builder = TransitionBuilder::new(initial, req.now);
        builder.request_dedupe_ops.push(RequestDedupeOp {
            request_id: req.request.request_id.clone(),
        });
        builder.emit(HistoryEventKind::WorkflowExecutionStarted {
            workflow_type: req.workflow_type,
            task_queue: req.task_queue,
            input: req.input,
            header: req.header,
            memo: req.memo.clone(),
            search_attributes: req.search_attributes.clone(),
            request_id: req.request.request_id.0,
            identity: req.request.caller_identity.unwrap_or_default(),
            continued_execution_run_id: req.continued_execution_run_id,
            first_execution_run_id: req.first_execution_run_id,
            retry_policy: req.retry_policy,
            attempt: req.attempt,
            workflow_execution_timeout: req.workflow_execution_timeout,
            workflow_run_timeout: req.workflow_run_timeout,
            workflow_task_timeout: req.workflow_task_timeout,
            parent_workflow_id: req.parent_workflow_id.clone(),
            parent_run_id: req.parent_run_id,
            parent_namespace_id: req.parent_namespace_id,
            parent_initiated_event_id: req.parent_initiated_event_id,
            root_workflow_id: event_root_workflow_id,
            root_run_id: event_root_run_id,
            original_execution_run_id: req.original_execution_run_id.or(Some(req.run_id)),
            continued_failure: req.continued_failure,
            last_completion_result: req.last_completion_result,
            cron_schedule: req.cron_schedule,
            workflow_start_delay: req.workflow_start_delay,
            completion_callbacks,
            user_metadata: req.user_metadata,
            links: req.links,
            priority: req.priority,
            versioning_info: initial_versioning_info,
            worker_deployment_name: None,
        });
        // The starting request id authors the WorkflowExecutionStarted event
        // (just emitted, so it is `last_event_id`). Recorded for
        // DescribeWorkflowExecution.WorkflowExtendedInfo.request_id_infos and for
        // the already-started error detail on a later conflicting start
        // (`WorkflowExecutionInfo.request_ids @ v1.31.0`). The request id was
        // moved into the started event above, so reuse the captured copy.
        builder.state.request_id_infos.insert(
            start_request_id,
            RequestIdInfo {
                event_id: builder.state.last_event_id,
                event_type: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
                buffered: false,
            },
        );
        builder.projection_ops.push(ProjectionOp::UpsertExecution {
            status: ExecutionStatus::Running,
            memo_patch: req.memo,
            search_attr_patch: req.search_attributes,
        });
        if let Some(delay) = positive_start_delay(req.workflow_start_delay) {
            let timer = TimerState {
                timer_id: WORKFLOW_START_DELAY_TIMER_ID.to_string(),
                started_event_id: 0,
                fire_at: req.now + delay,
            };
            builder
                .state
                .timers
                .insert(timer.timer_id.clone(), timer.clone());
            builder.timer_ops.push(TimerOp::Upsert(timer));
        } else {
            builder.schedule_workflow_task();
            if let Some(identity) = req.reserved_poller_identity {
                builder.start_pending_workflow_task(identity);
            }
        }
        Ok(builder.finish())
    }

    /// Atomically create a run and deliver a signal in one transition,
    /// so the signal is never lost to a race with a separate start.
    fn apply_signal_with_start(
        &self,
        loaded: LoadedRun,
        req: SignalWithStartRequest,
    ) -> Result<Transition, Reject> {
        if !matches!(loaded, LoadedRun::Absent) {
            return Err(Reject::RunAlreadyExists);
        }

        let canonical_root_workflow_id = req
            .root_workflow_id
            .clone()
            .unwrap_or_else(|| req.workflow_id.clone());
        let canonical_root_run_id = req.root_run_id.unwrap_or(req.run_id);
        let event_root_workflow_id = req.root_workflow_id.clone();
        let event_root_run_id = req.root_run_id;
        let initial_versioning_info = initial_versioning_info(req.versioning_override.clone());
        let initial = WorkflowState {
            run_key: req.run_key,
            namespace_id: req.namespace_id,
            workflow_id: req.workflow_id,
            run_id: req.run_id,
            workflow_type: req.workflow_type.clone(),
            task_queue: req.task_queue.clone(),
            deployment: req.deployment,
            build_id: req.build_id,
            versioning_info: initial_versioning_info.clone(),
            worker_deployment_name: None,
            status: ExecutionStatus::Running,
            transition_seq: TransitionSeq::ZERO,
            last_event_id: 0,
            next_workflow_task_seq: LogicalTaskSeq::ONE,
            pending_workflow_task: None,
            previous_started_event_id: 0,
            workflow_task_attempt: 1,
            sticky: None,
            pause_info: None,
            cancel_requested: false,
            wft_stamp: 0,
            memo: req.memo.clone(),
            search_attributes: req.search_attributes.clone(),
            workflow_execution_timeout: req.workflow_execution_timeout,
            workflow_run_timeout: req.workflow_run_timeout,
            workflow_task_timeout: req.workflow_task_timeout,
            retry_policy: req.retry_policy.clone(),
            attempt: req.attempt,
            first_execution_run_id: req.first_execution_run_id,
            original_execution_run_id: req.original_execution_run_id.or(Some(req.run_id)),
            parent_run_key: req.parent_run_key,
            parent_workflow_id: req.parent_workflow_id.clone(),
            parent_run_id: req.parent_run_id,
            parent_namespace_id: req.parent_namespace_id,
            parent_initiated_event_id: req.parent_initiated_event_id,
            root_workflow_id: Some(canonical_root_workflow_id),
            root_run_id: Some(canonical_root_run_id),
            last_completion_result: req.last_completion_result.clone(),
            activities: BTreeMap::new(),
            timers: BTreeMap::new(),
            children: BTreeMap::new(),
            pending_external_signals: BTreeMap::new(),
            pending_external_cancels: BTreeMap::new(),
            pending_updates: BTreeMap::new(),
            admitted_updates: std::collections::HashSet::new(),
            pending_nexus_operations: BTreeMap::new(),
            completion_callbacks: Vec::new(),
            user_metadata: req.user_metadata.clone(),
            links: req.links.clone(),
            workflow_start_delay: req.workflow_start_delay,
            priority: req.priority.clone(),
            started_at: req.now,
            first_run_started_at: req.first_run_started_at,
            closed_at: None,
            close_result: None,
            close_failure: None,
            request_id_infos: BTreeMap::new(),
            buffered_events: Vec::new(),
        };

        let mut builder = TransitionBuilder::new(initial, req.now);
        builder.request_dedupe_ops.push(RequestDedupeOp {
            request_id: req.request.request_id.clone(),
        });
        builder.emit(HistoryEventKind::WorkflowExecutionStarted {
            workflow_type: req.workflow_type,
            task_queue: req.task_queue,
            input: req.input,
            header: req.header.clone(),
            memo: req.memo.clone(),
            search_attributes: req.search_attributes.clone(),
            request_id: req.request.request_id.0.clone(),
            identity: req.request.caller_identity.clone().unwrap_or_default(),
            continued_execution_run_id: req.continued_execution_run_id,
            first_execution_run_id: req.first_execution_run_id,
            retry_policy: req.retry_policy,
            attempt: req.attempt,
            workflow_execution_timeout: req.workflow_execution_timeout,
            workflow_run_timeout: req.workflow_run_timeout,
            workflow_task_timeout: req.workflow_task_timeout,
            parent_workflow_id: req.parent_workflow_id,
            parent_run_id: req.parent_run_id,
            parent_namespace_id: req.parent_namespace_id,
            parent_initiated_event_id: req.parent_initiated_event_id,
            root_workflow_id: event_root_workflow_id,
            root_run_id: event_root_run_id,
            original_execution_run_id: req.original_execution_run_id.or(Some(req.run_id)),
            continued_failure: req.continued_failure,
            last_completion_result: req.last_completion_result,
            cron_schedule: req.cron_schedule,
            workflow_start_delay: req.workflow_start_delay,
            completion_callbacks: Vec::new(),
            user_metadata: req.user_metadata,
            links: req.links.clone(),
            priority: req.priority,
            versioning_info: initial_versioning_info,
            worker_deployment_name: None,
        });
        builder.emit(HistoryEventKind::WorkflowExecutionSignaled {
            signal_name: req.signal_name,
            input: req.signal_input,
            header: req.header,
            links: req.links,
            request_id: req.request.request_id.0,
            identity: req.request.caller_identity,
        });
        builder.projection_ops.push(ProjectionOp::UpsertExecution {
            status: ExecutionStatus::Running,
            memo_patch: req.memo,
            search_attr_patch: req.search_attributes,
        });
        if let Some(delay) = positive_start_delay(req.workflow_start_delay) {
            let timer = TimerState {
                timer_id: WORKFLOW_START_DELAY_TIMER_ID.to_string(),
                started_event_id: 0,
                fire_at: req.now + delay,
            };
            builder
                .state
                .timers
                .insert(timer.timer_id.clone(), timer.clone());
            builder.timer_ops.push(TimerOp::Upsert(timer));
        } else {
            builder.schedule_workflow_task();
        }
        Ok(builder.finish())
    }

    /// Create a workflow with an update admitted in the SAME transition —
    /// Update-with-Start's fresh-start leg (`Command::StartAndUpdate`,
    /// raised + accepted in `.kiro/specs/api-conformance-multi-operation`;
    /// the `SignalWithStart` atomicity precedent). The start applies exactly
    /// as a standalone start (same events, same first WFT); the admission is
    /// pure state — v1.31.0 writes no update event here, and the first
    /// workflow task delivers the update
    /// (`workflowLeaseCallback` admission before the persistence write,
    /// multioperation/api.go @ v1.31.0).
    fn apply_start_and_update(
        &self,
        loaded: LoadedRun,
        req: StartAndUpdateRequest,
    ) -> Result<Transition, Reject> {
        let mut transition = self.apply_start(loaded, req.start)?;
        transition.next_state.admitted_updates.insert(req.update_id);
        Ok(transition)
    }

    /// Deliver an external signal to a running workflow.
    /// Only schedules a new WFT if none is already pending — see the
    /// "at most one outstanding WFT" invariant comment below.
    fn apply_signal(&self, loaded: LoadedRun, req: SignalRequest) -> Result<Transition, Reject> {
        let state = expect_open(loaded)?;
        let mut builder = TransitionBuilder::new(state, req.now);
        // The dedupe op is emitted at *admission* even when the event is
        // buffered below: idempotency of `SignalWorkflowExecution` is anchored
        // to the request id at durable acceptance, not to the signal's
        // eventual history position (spec: kernel-event-buffering Req 2.1.3).
        builder.request_dedupe_ops.push(RequestDedupeOp {
            request_id: req.request.request_id.clone(),
        });

        let kind = HistoryEventKind::WorkflowExecutionSignaled {
            signal_name: req.signal_name,
            input: req.input,
            header: req.header,
            links: req.links,
            request_id: req.request.request_id.0,
            identity: req.request.caller_identity,
        };
        if should_buffer(&builder.state, &kind) {
            // A worker holds the started WFT and its view of history is frozen
            // at started_event_id; the signal is held on state and flushed when
            // the WFT closes (`bufferEvent`, event_store.go:263 @ v1.31.0). No
            // new WFT is scheduled — a started WFT is by definition pending,
            // and the flush-on-close path schedules the follow-up delivery
            // (Req 2.2).
            builder.buffer(kind);
            return Ok(builder.finish());
        }
        builder.emit(kind);

        // Insight: Tokeira keeps the "at most one outstanding workflow task"
        // invariant because it dramatically reduces wakeup amplification during
        // signal floods without weakening per-run correctness.
        //
        // A run still inside its first-WFT backoff window (client start delay
        // or cron initial interval — the start-delay timer is armed and no WFT
        // has ever existed) records the signal but does NOT create a workflow
        // task; the backoff timer schedules the first WFT when it fires
        // (`CreateWorkflowTask := !IsWorkflowPendingOnWorkflowTaskBackoff`,
        // signalworkflow/api.go:70-74 + mutable_state_impl.go:2368-2374
        // @ v1.31.0).
        if builder.state.pending_workflow_task.is_none()
            && !builder
                .state
                .timers
                .contains_key(WORKFLOW_START_DELAY_TIMER_ID)
        {
            builder.schedule_workflow_task();
        }

        Ok(builder.finish())
    }

    /// Admit a workflow update request. Updates go through a two-phase
    /// lifecycle (admitted → accepted/rejected) because the worker must
    /// validate the update before it becomes durable.
    fn apply_update(&self, loaded: LoadedRun, req: UpdateRequest) -> Result<Transition, Reject> {
        let state = expect_open(loaded)?;
        if state.status == ExecutionStatus::Paused {
            return Err(Reject::WorkflowPaused);
        }
        // Reject duplicate update IDs — check both pending_updates
        // (already accepted) and admitted_updates (awaiting worker acceptance).
        if state.pending_updates.contains_key(&req.update_id) {
            return Err(Reject::DuplicateUpdateId(req.update_id));
        }
        if state.admitted_updates.contains(&req.update_id) {
            return Err(Reject::DuplicateUpdateId(req.update_id));
        }

        let mut builder = TransitionBuilder::new(state, req.now);
        builder.request_dedupe_ops.push(RequestDedupeOp {
            request_id: req.request.request_id.clone(),
        });

        // Track that this update has been admitted but not yet accepted
        // by the worker. The UpdateAccepted event is written later when
        // the worker sends an Acceptance protocol message.
        builder.state.admitted_updates.insert(req.update_id.clone());

        if builder.state.pending_workflow_task.is_none() {
            builder.schedule_workflow_task();
        }

        Ok(builder.finish())
    }

    /// Record a cancellation request. This does not close the workflow —
    /// the worker decides how to honour the request during its next WFT.
    fn apply_cancel(&self, loaded: LoadedRun, req: CancelRequest) -> Result<Transition, Reject> {
        let state = match loaded {
            LoadedRun::Absent => return Err(Reject::MissingRun),
            LoadedRun::Existing(state) => state,
        };
        // A cancel of a closed run or an already-cancel-requested run is a
        // success NO-OP — no event, no WFT: "the request to cancel this
        // workflow is a success even if the target workflow has already
        // finished", and repeat requests short-circuit on `IsCancelRequested`
        // (requestcancelworkflow/api.go:44-53,74-81 @ v1.31.0). This is also
        // what turns a cancel-external against a finished target into the
        // source's ExternalWorkflowExecutionCancelRequested SUCCESS event.
        if !state.is_open() || state.cancel_requested {
            return Ok(TransitionBuilder::new(state, req.now).finish());
        }
        let mut builder = TransitionBuilder::new(state, req.now);
        builder.request_dedupe_ops.push(RequestDedupeOp {
            request_id: req.request.request_id.clone(),
        });
        let kind = HistoryEventKind::WorkflowExecutionCancelRequested {
            reason: req.reason,
            external_workflow_execution: req.external_initiator,
            external_initiated_event_id: req.external_initiated_event_id,
            identity: req.request.caller_identity.clone().unwrap_or_default(),
            request_id: req.request.request_id.0,
        };
        // `cancel_requested` flips at admission even when the event buffers:
        // v1.31.0 mutates the execution info immediately while the event sits
        // in the buffer (`ReplicateWorkflowExecutionCancelRequestedEvent`,
        // mutable_state_impl.go @ v1.31.0).
        builder.state.cancel_requested = true;
        if should_buffer(&builder.state, &kind) {
            builder.buffer(kind);
            return Ok(builder.finish());
        }
        builder.emit(kind);

        if builder.state.pending_workflow_task.is_none() {
            builder.schedule_workflow_task();
        }

        Ok(builder.finish())
    }

    /// Forcefully close the workflow. Unlike cancel, terminate is
    /// immediate — the worker gets no say. All pending activities and
    /// timers are deleted because they can never resolve.
    ///
    /// When a WFT is started, it is force-failed first (cause
    /// `ForceCloseCommand`) and buffered events flush before the terminated
    /// event, matching `TerminateWorkflow`
    /// (`service/history/workflow/util.go:115 @ v1.31.0`).
    fn apply_terminate(
        &self,
        loaded: LoadedRun,
        req: TerminateRequest,
    ) -> Result<Transition, Reject> {
        let state = expect_open(loaded)?;
        let mut builder = TransitionBuilder::new(state, req.now);
        builder.request_dedupe_ops.push(RequestDedupeOp {
            request_id: req.request.request_id.clone(),
        });
        builder.terminate_run(req.reason, req.details, req.identity);
        Ok(builder.finish())
    }

    /// Force-close-then-terminate driven by `RespondWorkflowTaskFailed` with
    /// cause `GRPC_MESSAGE_TOO_LARGE`
    /// (`respondworkflowtaskfailed/api.go:88 @ v1.31.0`): instead of the WFT
    /// retry path, the started WFT is failed with `ForceCloseCommand`,
    /// buffered events flush, and the run terminates. Fencing matches the
    /// WFT-failed path so a stale task token is rejected (spec:
    /// kernel-event-buffering Req 4.2, 5.1).
    fn apply_terminate_on_workflow_task_failed(
        &self,
        loaded: LoadedRun,
        req: TerminateOnWorkflowTaskFailedRequest,
    ) -> Result<Transition, Reject> {
        let state = expect_open(loaded)?;
        let pending = state
            .pending_workflow_task
            .clone()
            .ok_or(Reject::NoPendingWorkflowTask)?;
        let started_event_id = pending
            .started_event_id
            .ok_or(Reject::WorkflowTaskNotStarted {
                logical_seq: pending.logical_seq.0,
            })?;
        if pending.logical_seq != req.logical_seq {
            return Err(Reject::WorkflowTaskSeqMismatch {
                expected: pending.logical_seq.0,
                got: req.logical_seq.0,
            });
        }
        if started_event_id != req.started_event_id {
            return Err(Reject::WorkflowTaskTokenMismatch);
        }

        let mut builder = TransitionBuilder::new(state, req.now);
        builder.request_dedupe_ops.push(RequestDedupeOp {
            request_id: req.request.request_id.clone(),
        });
        builder.terminate_run(req.reason, None, req.identity);
        Ok(builder.finish())
    }

    /// Freeze the workflow so no new WFTs are dispatched. Bumps the
    /// `wft_stamp` and activity stamps to invalidate any in-flight
    /// task tokens, preventing stale completions from landing.
    fn apply_pause_workflow(
        &self,
        loaded: LoadedRun,
        req: PauseWorkflowRequest,
    ) -> Result<Transition, Reject> {
        let state = expect_open(loaded)?;

        if state.status == ExecutionStatus::Paused {
            if let Some(info) = &state.pause_info
                && info.request_id == req.request.request_id.0
            {
                return Ok(TransitionBuilder::new(state, req.now).finish());
            }
            return Err(Reject::AlreadyPaused);
        }

        let mut builder = TransitionBuilder::new(state, req.now);
        builder.request_dedupe_ops.push(RequestDedupeOp {
            request_id: req.request.request_id.clone(),
        });
        builder.emit(HistoryEventKind::WorkflowExecutionPaused {
            identity: req.identity.clone(),
            reason: req.reason.clone(),
            request_id: req.request.request_id.0.clone(),
        });
        builder.state.status = ExecutionStatus::Paused;
        builder.state.pause_info = Some(PauseInfo {
            pause_time: req.now,
            identity: req.identity,
            reason: req.reason,
            request_id: req.request.request_id.0,
        });
        builder.state.wft_stamp += 1;

        let activity_ids: Vec<_> = builder.state.activities.keys().cloned().collect();
        for activity_id in activity_ids {
            if let Some(activity) = builder.state.activities.get_mut(&activity_id) {
                activity.stamp += 1;
                builder
                    .activity_ops
                    .push(ActivityOp::Upsert(activity.clone()));
            }
        }

        builder.projection_ops.push(ProjectionOp::UpsertExecution {
            status: ExecutionStatus::Paused,
            memo_patch: builder.state.memo.clone(),
            search_attr_patch: builder.state.search_attributes.clone(),
        });
        Ok(builder.finish())
    }

    /// Resume a paused workflow. Re-enqueues every pending activity
    /// because their dispatch ops were suppressed while paused.
    fn apply_unpause_workflow(
        &self,
        loaded: LoadedRun,
        req: UnpauseWorkflowRequest,
    ) -> Result<Transition, Reject> {
        let state = expect_open(loaded)?;
        if state.status != ExecutionStatus::Paused {
            return Err(Reject::NotPaused);
        }

        let mut builder = TransitionBuilder::new(state, req.now);
        builder.request_dedupe_ops.push(RequestDedupeOp {
            request_id: req.request.request_id.clone(),
        });
        builder.emit(HistoryEventKind::WorkflowExecutionUnpaused {
            identity: req.identity,
            reason: req.reason,
            request_id: req.request.request_id.0,
        });
        builder.state.status = ExecutionStatus::Running;
        builder.state.pause_info = None;
        builder.state.wft_stamp += 1;

        let activity_ids: Vec<_> = builder.state.activities.keys().cloned().collect();
        for activity_id in activity_ids {
            let snapshot = {
                let activity = builder
                    .state
                    .activities
                    .get_mut(&activity_id)
                    .expect("activity must exist");
                activity.stamp += 1;
                activity.clone()
            };
            builder
                .activity_ops
                .push(ActivityOp::Upsert(snapshot.clone()));
            builder.dispatch_ops.push(DispatchOp::EnqueueActivityTask {
                queue: QueueKey {
                    namespace_id: builder.state.namespace_id,
                    task_queue: snapshot.task_queue.clone(),
                    task_kind: tokeira_types::TaskKind::Activity,
                    deployment: snapshot
                        .deployment
                        .clone()
                        .or_else(|| builder.state.deployment.clone()),
                    build_id: snapshot
                        .build_id
                        .clone()
                        .or_else(|| builder.state.build_id.clone()),
                },
                activity_id: snapshot.activity_id.clone(),
                input: snapshot.input.clone(),
                schedule_event_id: snapshot.schedule_event_id,
                attempt: snapshot.attempt,
                dispatch_revision: builder
                    .state
                    .versioning_info
                    .as_ref()
                    .map(|info| info.revision_number)
                    .unwrap_or_default(),
                schedule_to_close_timeout: snapshot.schedule_to_close_timeout,
                schedule_to_start_timeout: snapshot.schedule_to_start_timeout,
                start_to_close_timeout: snapshot.start_to_close_timeout,
                heartbeat_timeout: snapshot.heartbeat_timeout,
            });
        }

        builder.projection_ops.push(ProjectionOp::UpsertExecution {
            status: ExecutionStatus::Running,
            memo_patch: builder.state.memo.clone(),
            search_attr_patch: builder.state.search_attributes.clone(),
        });
        if builder.state.pending_workflow_task.is_none() {
            builder.schedule_workflow_task();
        }
        Ok(builder.finish())
    }

    /// Hot-patch activity options (timeouts, task queue) without
    /// cancelling and re-scheduling the activity.
    fn apply_update_activity_options(
        &self,
        loaded: LoadedRun,
        req: UpdateActivityOptionsRequest,
    ) -> Result<Transition, Reject> {
        let state = expect_open(loaded)?;
        let mut builder = TransitionBuilder::new(state, req.now);
        builder
            .state
            .activities
            .get(&req.activity_id)
            .ok_or_else(|| Reject::UnknownActivity(req.activity_id.clone()))?;
        builder.request_dedupe_ops.push(RequestDedupeOp {
            request_id: req.request.request_id.clone(),
        });

        let snapshot = {
            let activity = builder
                .state
                .activities
                .get_mut(&req.activity_id)
                .expect("activity must exist");
            match req.task_queue {
                FieldChange::Set(task_queue) => activity.task_queue = task_queue,
                FieldChange::Clear | FieldChange::Unchanged => {}
            }
            match req.schedule_to_close_timeout {
                FieldChange::Set(v) => activity.schedule_to_close_timeout = v,
                FieldChange::Clear => activity.schedule_to_close_timeout = None,
                FieldChange::Unchanged => {}
            }
            match req.schedule_to_start_timeout {
                FieldChange::Set(v) => activity.schedule_to_start_timeout = v,
                FieldChange::Clear => activity.schedule_to_start_timeout = None,
                FieldChange::Unchanged => {}
            }
            match req.start_to_close_timeout {
                FieldChange::Set(v) => activity.start_to_close_timeout = v,
                FieldChange::Clear => activity.start_to_close_timeout = None,
                FieldChange::Unchanged => {}
            }
            match req.heartbeat_timeout {
                FieldChange::Set(v) => activity.heartbeat_timeout = v,
                FieldChange::Clear => activity.heartbeat_timeout = None,
                FieldChange::Unchanged => {}
            }
            activity.stamp += 1;
            activity.clone()
        };
        builder.activity_ops.push(ActivityOp::Upsert(snapshot));
        Ok(builder.finish())
    }

    /// Pause a single activity. The activity stays in the pending set
    /// but will not be dispatched until unpaused.
    fn apply_pause_activity(
        &self,
        loaded: LoadedRun,
        req: PauseActivityRequest,
    ) -> Result<Transition, Reject> {
        let state = expect_open(loaded)?;
        let mut builder = TransitionBuilder::new(state, req.now);
        builder
            .state
            .activities
            .get(&req.activity_id)
            .ok_or_else(|| Reject::UnknownActivity(req.activity_id.clone()))?;
        builder.request_dedupe_ops.push(RequestDedupeOp {
            request_id: req.request.request_id.clone(),
        });
        let snapshot = {
            let activity = builder
                .state
                .activities
                .get_mut(&req.activity_id)
                .expect("activity must exist");
            activity.pause_info = Some(ActivityPauseInfo {
                pause_time: req.now,
                identity: req.identity,
                reason: req.reason,
            });
            activity.stamp += 1;
            activity.clone()
        };
        builder.activity_ops.push(ActivityOp::Upsert(snapshot));
        Ok(builder.finish())
    }

    /// Resume a paused activity and re-enqueue it for dispatch
    /// (unless the whole workflow is paused).
    fn apply_unpause_activity(
        &self,
        loaded: LoadedRun,
        req: UnpauseActivityRequest,
    ) -> Result<Transition, Reject> {
        let state = expect_open(loaded)?;
        let mut builder = TransitionBuilder::new(state, req.now);
        builder.request_dedupe_ops.push(RequestDedupeOp {
            request_id: req.request.request_id.clone(),
        });
        let snapshot = {
            let activity = builder
                .state
                .activities
                .get_mut(&req.activity_id)
                .ok_or_else(|| Reject::UnknownActivity(req.activity_id.clone()))?;
            if activity.pause_info.is_none() {
                return Err(Reject::ActivityNotPaused(req.activity_id));
            }
            activity.pause_info = None;
            activity.stamp += 1;
            activity.clone()
        };
        if builder.state.status != ExecutionStatus::Paused {
            builder.dispatch_ops.push(DispatchOp::EnqueueActivityTask {
                queue: QueueKey {
                    namespace_id: builder.state.namespace_id,
                    task_queue: snapshot.task_queue.clone(),
                    task_kind: tokeira_types::TaskKind::Activity,
                    deployment: snapshot
                        .deployment
                        .clone()
                        .or_else(|| builder.state.deployment.clone()),
                    build_id: snapshot
                        .build_id
                        .clone()
                        .or_else(|| builder.state.build_id.clone()),
                },
                activity_id: snapshot.activity_id.clone(),
                input: snapshot.input.clone(),
                schedule_event_id: snapshot.schedule_event_id,
                attempt: snapshot.attempt,
                dispatch_revision: builder
                    .state
                    .versioning_info
                    .as_ref()
                    .map(|info| info.revision_number)
                    .unwrap_or_default(),
                schedule_to_close_timeout: snapshot.schedule_to_close_timeout,
                schedule_to_start_timeout: snapshot.schedule_to_start_timeout,
                start_to_close_timeout: snapshot.start_to_close_timeout,
                heartbeat_timeout: snapshot.heartbeat_timeout,
            });
        }
        builder.activity_ops.push(ActivityOp::Upsert(snapshot));
        Ok(builder.finish())
    }

    /// Reset an activity's attempt counter and re-dispatch it,
    /// giving the worker a fresh start without losing the schedule event.
    fn apply_reset_activity(
        &self,
        loaded: LoadedRun,
        req: ResetActivityRequest,
    ) -> Result<Transition, Reject> {
        let state = expect_open(loaded)?;
        let mut builder = TransitionBuilder::new(state, req.now);
        builder.request_dedupe_ops.push(RequestDedupeOp {
            request_id: req.request.request_id.clone(),
        });
        let snapshot = {
            let activity = builder
                .state
                .activities
                .get_mut(&req.activity_id)
                .ok_or_else(|| Reject::UnknownActivity(req.activity_id.clone()))?;
            if req.reset_heartbeat {
                activity.heartbeat_details = None;
            }
            activity.attempt = 1;
            activity.stamp += 1;
            activity.clone()
        };
        if builder.state.status != ExecutionStatus::Paused {
            builder.dispatch_ops.push(DispatchOp::EnqueueActivityTask {
                queue: QueueKey {
                    namespace_id: builder.state.namespace_id,
                    task_queue: snapshot.task_queue.clone(),
                    task_kind: tokeira_types::TaskKind::Activity,
                    deployment: snapshot
                        .deployment
                        .clone()
                        .or_else(|| builder.state.deployment.clone()),
                    build_id: snapshot
                        .build_id
                        .clone()
                        .or_else(|| builder.state.build_id.clone()),
                },
                activity_id: snapshot.activity_id.clone(),
                input: snapshot.input.clone(),
                schedule_event_id: snapshot.schedule_event_id,
                attempt: snapshot.attempt,
                dispatch_revision: builder
                    .state
                    .versioning_info
                    .as_ref()
                    .map(|info| info.revision_number)
                    .unwrap_or_default(),
                schedule_to_close_timeout: snapshot.schedule_to_close_timeout,
                schedule_to_start_timeout: snapshot.schedule_to_start_timeout,
                start_to_close_timeout: snapshot.start_to_close_timeout,
                heartbeat_timeout: snapshot.heartbeat_timeout,
            });
        }
        builder.activity_ops.push(ActivityOp::Upsert(snapshot));
        Ok(builder.finish())
    }

    /// Fork the workflow history at a prior event, closing this run
    /// and pointing to a new run that will replay from the fork point.
    fn apply_reset(&self, loaded: LoadedRun, req: ResetRequest) -> Result<Transition, Reject> {
        let state = expect_open(loaded)?;

        if req.fork_event_id <= 0 {
            return Err(Reject::ResetConstraintViolation {
                reason: format!("fork_event_id must be positive, got {}", req.fork_event_id),
            });
        }
        if req.fork_event_id > state.last_event_id {
            return Err(Reject::ResetConstraintViolation {
                reason: format!(
                    "fork_event_id {} exceeds last_event_id {}",
                    req.fork_event_id, state.last_event_id
                ),
            });
        }

        let (scheduled_event_id, started_event_id) = match &state.pending_workflow_task {
            Some(pending) => (
                pending.scheduled_event_id,
                pending.started_event_id.unwrap_or(0),
            ),
            None => (0, 0),
        };

        let logical_seq = state.next_workflow_task_seq;
        let base_run_id = state.run_id;

        let mut builder = TransitionBuilder::new(state, req.now);
        builder.request_dedupe_ops.push(RequestDedupeOp {
            request_id: req.request.request_id.clone(),
        });
        builder.emit(HistoryEventKind::WorkflowTaskFailed {
            logical_seq,
            scheduled_event_id,
            started_event_id,
            failure_cause: crate::command::WorkflowTaskFailedCause::ResetWorkflow,
            failure_details: None,
            identity: tokeira_types::WorkerIdentity("reset".into()),
            base_run_id: Some(base_run_id),
            new_run_id: Some(req.new_run_id),
            fork_event_version: None,
            fork_event_id: Some(req.fork_event_id),
        });
        builder.close(ExecutionStatus::Terminated);

        let activities = std::mem::take(&mut builder.state.activities);
        for (activity_id, _) in activities {
            builder
                .activity_ops
                .push(ActivityOp::Delete { activity_id });
        }

        let timers = std::mem::take(&mut builder.state.timers);
        for (timer_id, _) in timers {
            builder.timer_ops.push(TimerOp::Delete { timer_id });
        }

        builder.apply_parent_close_policy();

        Ok(builder.finish())
    }

    /// Mutate versioning override and completion callbacks on a live run.
    fn apply_update_execution_options(
        &self,
        loaded: LoadedRun,
        req: UpdateExecutionOptionsRequest,
    ) -> Result<Transition, Reject> {
        let state = expect_open(loaded)?;
        let mut completion_callbacks = req.completion_callbacks;
        if let FieldChange::Set(callbacks) = &mut completion_callbacks {
            stamp_callback_registration_times(callbacks, req.now);
        }
        let mut attached_completion_callbacks = req.attached_completion_callbacks;
        stamp_callback_registration_times(&mut attached_completion_callbacks, req.now);
        let mut builder = TransitionBuilder::new(state, req.now);
        builder.request_dedupe_ops.push(RequestDedupeOp {
            request_id: req.request.request_id.clone(),
        });
        // Captured before the emit moves it; an attached request id (set when a
        // UseExisting start attaches to this run) authors this options-updated
        // event and must appear in request_id_infos (Req 5.3).
        let attached_request_id_for_map = req.attached_request_id.clone();
        builder.emit(HistoryEventKind::WorkflowExecutionOptionsUpdated {
            versioning_override: req.versioning_override.clone(),
            completion_callbacks: completion_callbacks.clone(),
            attached_completion_callbacks: attached_completion_callbacks.clone(),
            attached_links: req.attached_links.clone(),
            attached_request_id: req.attached_request_id,
        });
        if let Some(attached_request_id) = attached_request_id_for_map {
            builder.state.request_id_infos.insert(
                attached_request_id,
                RequestIdInfo {
                    event_id: builder.state.last_event_id,
                    event_type: EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
                    buffered: false,
                },
            );
        }

        match req.versioning_override {
            FieldChange::Set(versioning_override) => {
                builder
                    .state
                    .set_versioning_override(Some(versioning_override));
            }
            FieldChange::Clear => {
                builder.state.set_versioning_override(None);
            }
            FieldChange::Unchanged => {}
        }

        match completion_callbacks {
            FieldChange::Set(callbacks) => {
                builder.state.completion_callbacks = callbacks;
            }
            FieldChange::Clear => {
                builder.state.completion_callbacks.clear();
            }
            FieldChange::Unchanged => {}
        }
        builder
            .state
            .completion_callbacks
            .extend(attached_completion_callbacks);
        builder.state.links.extend(req.attached_links);

        Ok(builder.finish())
    }

    /// Close the workflow because a server-enforced timeout fired.
    /// This is not a worker decision — the runtime drives it.
    fn apply_workflow_execution_timed_out(
        &self,
        loaded: LoadedRun,
        req: WorkflowExecutionTimedOutRequest,
    ) -> Result<Transition, Reject> {
        let state = expect_open(loaded)?;
        let mut builder = TransitionBuilder::new(state, req.now);
        // A workflow-level timeout is a forced close like terminate: a started
        // WFT is failed first (ForceCloseCommand) and buffered events flush
        // before the timed-out event (`TimeoutWorkflow`,
        // `service/history/workflow/util.go:71 @ v1.31.0`).
        builder.force_close_started_workflow_task();
        builder.flush_buffered();
        builder.emit(HistoryEventKind::WorkflowExecutionTimedOut {
            timeout_type: req.timeout_type,
            retry_state: req.retry_state,
            new_execution_run_id: req.new_execution_run_id,
        });
        builder.close(ExecutionStatus::TimedOut);

        let activities = std::mem::take(&mut builder.state.activities);
        for (activity_id, _) in activities {
            builder
                .activity_ops
                .push(ActivityOp::Delete { activity_id });
        }

        let timers = std::mem::take(&mut builder.state.timers);
        for (timer_id, _) in timers {
            builder.timer_ops.push(TimerOp::Delete { timer_id });
        }

        builder.apply_parent_close_policy();

        Ok(builder.finish())
    }

    /// Mark a pending WFT as started. Validates the logical sequence
    /// to prevent stale dispatches from mutating state.
    fn apply_workflow_task_started(
        &self,
        loaded: LoadedRun,
        req: StartWorkflowTaskRequest,
    ) -> Result<Transition, Reject> {
        let state = expect_open(loaded)?;
        let pending = state
            .pending_workflow_task
            .clone()
            .ok_or(Reject::NoPendingWorkflowTask)?;

        if pending.logical_seq != req.logical_seq {
            return Err(Reject::WorkflowTaskSeqMismatch {
                expected: pending.logical_seq.0,
                got: req.logical_seq.0,
            });
        }
        if pending.started_event_id.is_some() {
            return Err(Reject::WorkflowTaskAlreadyStarted {
                logical_seq: pending.logical_seq.0,
            });
        }

        let mut builder = TransitionBuilder::new(state, req.now);
        let attempt = pending.attempt.max(1);
        // Transient (attempt>1) start persists no WorkflowTaskStarted event: the started
        // id is virtual (scheduled_event_id + 1) and last_event_id is unchanged
        // (workflow_task_state_machine.go:480 @ v1.31.0). Conversion (rule ii): if new
        // events landed after the virtual scheduling — the virtual scheduled id no longer
        // equals last_event_id + 1 — convert back to a NORMAL attempt-1 task with real
        // Scheduled + Started, since the frozen virtual suffix is now invalid
        // (workflow_task_state_machine.go:559-576 @ v1.31.0).
        let transient = attempt > 1;
        let new_events_since_schedule =
            transient && pending.scheduled_event_id != builder.state.last_event_id + 1;
        let (scheduled_event_id, started_event_id) = if transient && !new_events_since_schedule {
            (pending.scheduled_event_id, pending.scheduled_event_id + 1)
        } else if new_events_since_schedule {
            builder.state.workflow_task_attempt = 1;
            let scheduled = builder.emit(HistoryEventKind::WorkflowTaskScheduled {
                logical_seq: pending.logical_seq,
                task_queue: builder.state.task_queue.clone(),
                workflow_task_timeout: builder.state.workflow_task_timeout,
                attempt: 1,
            });
            let started = builder.emit(HistoryEventKind::WorkflowTaskStarted {
                logical_seq: pending.logical_seq,
                scheduled_event_id: scheduled,
                attempt: 1,
                identity: req.worker_identity.clone(),
                request_id: req.request_id,
                history_size_bytes: req.history_size_bytes,
                suggest_continue_as_new: req.suggest_continue_as_new,
            });
            (scheduled, started)
        } else {
            let started = builder.emit(HistoryEventKind::WorkflowTaskStarted {
                logical_seq: pending.logical_seq,
                scheduled_event_id: pending.scheduled_event_id,
                attempt,
                identity: req.worker_identity.clone(),
                request_id: req.request_id,
                history_size_bytes: req.history_size_bytes,
                suggest_continue_as_new: req.suggest_continue_as_new,
            });
            (pending.scheduled_event_id, started)
        };

        let current = builder
            .state
            .pending_workflow_task
            .as_mut()
            .expect("validated pending workflow task must still exist");
        current.scheduled_event_id = scheduled_event_id;
        current.started_event_id = Some(started_event_id);
        current.started_at = Some(req.now);
        current.attempt = builder.state.workflow_task_attempt;

        if let Some(ttl) = req.sticky_ttl {
            // Poll-side affinity is a sync-match HINT only: no sticky queue
            // and no S2S deadline (empty queue name disables sticky
            // dispatch). Real sticky attributes arrive on the completion
            // (sticky raise S1).
            builder.state.sticky = Some(StickyAffinity {
                worker_identity: req.worker_identity,
                expires_at: req.now + ttl,
                sticky_queue: TaskQueueName(String::new()),
                schedule_to_start_timeout: Duration::ZERO,
            });
        }

        if let Some(target) = req.deployment_transition {
            builder
                .state
                .start_version_transition(
                    target,
                    req.deployment_transition_revision_number
                        .unwrap_or_default(),
                )
                .map_err(|error| match error {
                    crate::state::VersionTransitionError::PinnedWorkflowCannotTransition => {
                        Reject::PinnedWorkflowCannotTransition
                    }
                })?;
        }

        Ok(builder.finish())
    }

    /// Start a Worker Deployment transition and ensure a WFT exists to observe it.
    ///
    /// This mirrors the activity-start coupling in Temporal v1.31.0
    /// `service/history/api/recordactivitytaskstarted/api.go:75`, where a
    /// transition-triggering activity start mutates mutable state and requests
    /// `CreateWorkflowTask`, and
    /// `service/history/workflow/mutable_state_impl.go:9060`, where
    /// `StartDeploymentTransition` reschedules pending WFTs. The runtime owns
    /// the live routing decision; this command only applies that decision in the
    /// deterministic kernel.
    fn apply_start_deployment_transition(
        &self,
        loaded: LoadedRun,
        req: crate::command::StartDeploymentTransitionRequest,
    ) -> Result<Transition, Reject> {
        let state = expect_open(loaded)?;
        let mut builder = TransitionBuilder::new(state, req.now);
        builder
            .state
            .start_version_transition(req.target, req.revision_number)
            .map_err(|error| match error {
                crate::state::VersionTransitionError::PinnedWorkflowCannotTransition => {
                    Reject::PinnedWorkflowCannotTransition
                }
            })?;
        if builder.state.pending_workflow_task.is_none() {
            builder.schedule_workflow_task();
        }
        Ok(builder.finish())
    }

    /// Complete a workflow task and apply the worker's command batch.
    ///
    /// This is the most complex transition because it bridges the
    /// worker's view of history (frozen at `started_event_id`) with
    /// events that may have arrived concurrently (signals, updates).
    /// Two independent conditions can trigger a follow-up WFT:
    ///
    /// 1. `force_new_workflow_task` — the worker explicitly asks for
    ///    another turn (e.g., local-activity heartbeat keep-alive).
    ///    This is checked first because the worker knows it needs
    ///    more work regardless of buffered events.
    ///
    /// 2. Buffered events — `pre_completion_last_event_id > started_event_id`
    ///    means events landed while the WFT was in flight. The worker
    ///    never saw them, so a fresh WFT is needed for correctness.
    ///    This is the mechanism that preserves the query consistency
    ///    model: queries piggyback on WFTs, so any state a query
    ///    might observe must have been processed by a WFT first.
    ///
    /// These are separate checks because `force_new_workflow_task`
    /// can be true even when no events were buffered (heartbeat
    /// keep-alive), and buffered events can exist even when the
    /// worker didn't request a new task.
    fn apply_workflow_task_completed(
        &self,
        loaded: LoadedRun,
        req: WorkflowTaskCompletedRequest,
        cron_continuation: Option<CronContinuation>,
        retry_continuation: Option<RetryContinuation>,
    ) -> Result<Transition, Reject> {
        let state = expect_open(loaded)?;
        let pending = state
            .pending_workflow_task
            .clone()
            .ok_or(Reject::NoPendingWorkflowTask)?;
        let started_event_id = pending
            .started_event_id
            .ok_or(Reject::WorkflowTaskNotStarted {
                logical_seq: pending.logical_seq.0,
            })?;

        if pending.logical_seq != req.token.logical_seq {
            return Err(Reject::WorkflowTaskSeqMismatch {
                expected: pending.logical_seq.0,
                got: req.token.logical_seq.0,
            });
        }
        if pending.attempt != req.token.attempt || started_event_id != req.token.started_event_id {
            return Err(Reject::WorkflowTaskTokenMismatch);
        }

        let mut builder = TransitionBuilder::new(state, req.now);

        // Capture last_event_id before emitting the completion event.
        // Events between started_event_id and this value arrived while
        // the WFT was in progress (e.g., signals) and need a fresh WFT.
        let pre_completion_last_event_id = builder.state.last_event_id;

        // Late materialization (Req B.6): a TRANSIENT (attempt>1) task's
        // Scheduled/Started events were never persisted; the whole retry chain
        // materializes exactly once — real WorkflowTaskScheduled +
        // WorkflowTaskStarted immediately before WorkflowTaskCompleted, with
        // contiguous ids (workflow_task_state_machine.go:750-800 @ v1.31.0).
        // With signals buffered during the started task the real ids equal the
        // virtual ones; if other events advanced history, fresh real ids are
        // assigned (v1.31.0 re-wires ids the same way at flush).
        let (scheduled_event_id, started_event_id) = if pending.attempt > 1 {
            // The materialized pair carries the task's ACTUAL schedule/start
            // times, not the completion time — the worker already observed
            // these timestamps on its poll response's synthesized transient
            // events, and the corpus asserts the persisted WorkflowTaskStarted
            // time equals what the worker saw (`workflowTask.ScheduledTime` /
            // `workflowTask.StartedTime` in `AddWorkflowTaskCompletedEvent`,
            // workflow_task_state_machine.go:768-800 @ v1.31.0).
            let scheduled = builder.emit_at(
                pending.scheduled_at,
                HistoryEventKind::WorkflowTaskScheduled {
                    logical_seq: pending.logical_seq,
                    task_queue: builder.state.task_queue.clone(),
                    workflow_task_timeout: builder.state.workflow_task_timeout,
                    attempt: pending.attempt,
                },
            );
            let started = builder.emit_at(
                pending.started_at.unwrap_or(req.now),
                HistoryEventKind::WorkflowTaskStarted {
                    logical_seq: pending.logical_seq,
                    scheduled_event_id: scheduled,
                    attempt: pending.attempt,
                    identity: req.identity.clone(),
                    request_id: format!("transient-materialize-{}", pending.logical_seq.0),
                    history_size_bytes: 0,
                    suggest_continue_as_new: false,
                },
            );
            (scheduled, started)
        } else {
            (pending.scheduled_event_id, started_event_id)
        };

        let wft_completed_event_id = builder.emit(HistoryEventKind::WorkflowTaskCompleted {
            logical_seq: req.token.logical_seq,
            scheduled_event_id,
            started_event_id,
            identity: req.identity.clone(),
            sdk_metadata: req.sdk_metadata,
            metering_metadata: req.metering_metadata,
            worker_version: req.worker_version,
            versioning_behavior: req.versioning_behavior,
            deployment_version: req.deployment_version.clone(),
            worker_deployment_name: req.worker_deployment_name.clone(),
        });
        builder.state.previous_started_event_id = started_event_id;
        builder.state.workflow_task_attempt = 1;
        builder.state.pending_workflow_task = None;
        builder.state.apply_wft_versioning(
            req.versioning_behavior,
            req.deployment_version,
            req.worker_deployment_name,
        );
        // Sticky raise S1: the completion's sticky attributes carry the
        // sticky QUEUE and the per-dispatch schedule-to-start timeout; a
        // completion without them clears the affinity (`ClearStickyTaskQueue`
        // on nil StickyAttributes @ v1.31.0).
        builder.state.sticky = req.sticky.map(|spec| StickyAffinity {
            worker_identity: req.identity,
            expires_at: req.now + spec.schedule_to_start_timeout,
            sticky_queue: spec.queue,
            schedule_to_start_timeout: spec.schedule_to_start_timeout,
        });

        let mut closed = false;
        for (index, command) in req.commands.into_iter().enumerate() {
            if closed {
                return Err(Reject::CommandsAfterClose { index });
            }
            // An invalid command REJECTS the completion; the runtime then
            // fails the WORKFLOW TASK with the carried cause and surfaces
            // InvalidArgument to the worker — mirroring v1.31.0's split
            // between the command handler (collects `wtFailedCause`) and the
            // API layer (`failWorkflowTask` + error return,
            // respondworkflowtaskcompleted/api.go:455-485,739-742).
            closed = apply_workflow_command(
                &mut builder,
                command,
                wft_completed_event_id,
                cron_continuation.as_ref(),
                retry_continuation.as_ref(),
            )?;
        }

        // Flush buffered events after the completion's command events and
        // before any follow-up WFT is scheduled: v1.31.0 flushes at the next
        // `AddWorkflowTaskScheduledEvent` / transaction close, so buffered
        // events land after command events and before the follow-up
        // WorkflowTaskScheduled (`AddWorkflowTaskScheduledEventAsHeartbeat`
        // flushes first, workflow_task_state_machine.go @ v1.31.0). If a close
        // command finished the run, `close()` already dropped the buffer —
        // v1.31.0 discards buffered events once the workflow finished
        // (`FlushBufferToCurrentBatch`, event_store.go:139 @ v1.31.0).
        let flushed = builder.flush_buffered();

        // Schedule a new WFT when the worker explicitly requests one
        // (heartbeat / local-activity keep-alive).
        if req.force_new_workflow_task
            && builder.state.is_open()
            && builder.state.pending_workflow_task.is_none()
        {
            builder.schedule_workflow_task();
        }

        // Schedule a new WFT if events arrived while this WFT was in
        // progress. Two sources: flushed buffered events (signals /
        // cancel-requested, which no longer advance last_event_id before
        // completion), and events still appended immediately during a started
        // WFT in Phase 1 (activity/child/Nexus resolutions — Phase 2 buffers
        // those too, at which point the numeric check becomes vestigial). The
        // worker only saw history up to started_event_id; anything beyond
        // needs a fresh WFT (spec: kernel-event-buffering Req 3.1.6).
        if builder.state.is_open()
            && builder.state.pending_workflow_task.is_none()
            && (flushed > 0 || pre_completion_last_event_id > started_event_id)
        {
            builder.schedule_workflow_task();
        }

        Ok(builder.finish())
    }

    /// Record that a worker picked up an activity task.
    ///
    /// Emits `ActivityTaskStarted` and records the started
    /// event ID back into `ActivityState` so that subsequent
    /// resolution events can reference it.
    pub fn apply_activity_started(
        &self,
        loaded: LoadedRun,
        activity_id: &str,
        identity: WorkerIdentity,
        now: OffsetDateTime,
    ) -> Result<Transition, Reject> {
        let state = expect_open(loaded)?;
        let activity = state
            .activities
            .get(activity_id)
            .cloned()
            .ok_or_else(|| Reject::UnknownActivity(activity_id.to_string()))?;

        let mut builder = TransitionBuilder::new(state, now);
        let started_event_id = builder.emit(HistoryEventKind::ActivityTaskStarted {
            activity_id: activity_id.to_string(),
            scheduled_event_id: activity.schedule_event_id,
            attempt: activity.attempt,
            identity: identity.clone(),
            request_id: format!("activity-start-{}-{}", activity_id, activity.attempt),
            last_failure: activity.last_failure.clone(),
        });

        if let Some(act) = builder.state.activities.get_mut(activity_id) {
            act.started_event_id = Some(started_event_id);
            act.started_at = Some(now);
            // `ai.StartedIdentity` (kernel raise K3) — Describe's primary
            // `LastWorkerIdentity` source (workflow/activity.go:159 @ v1.31.0).
            act.started_identity = Some(identity);
        }

        Ok(builder.finish())
    }

    /// Remove a resolved activity from the pending set and wake the
    /// workflow so it can observe the result.
    fn apply_activity_resolved(
        &self,
        loaded: LoadedRun,
        req: ActivityResolvedRequest,
    ) -> Result<Transition, Reject> {
        let state = expect_open(loaded)?;
        let activity = state
            .activities
            .get(&req.activity_id)
            .cloned()
            .ok_or_else(|| Reject::UnknownActivity(req.activity_id.clone()))?;

        let mut builder = TransitionBuilder::new(state, req.now);
        match req.resolution {
            ActivityResolution::Completed { result } => {
                builder.emit(HistoryEventKind::ActivityTaskCompleted {
                    activity_id: activity.activity_id.clone(),
                    scheduled_event_id: activity.schedule_event_id,
                    started_event_id: activity.started_event_id.unwrap_or(0),
                    identity: req.worker_identity.clone(),
                    result,
                });
            }
            ActivityResolution::Failed {
                failure,
                retry_state,
            } => {
                builder.emit(HistoryEventKind::ActivityTaskFailed {
                    activity_id: activity.activity_id.clone(),
                    scheduled_event_id: activity.schedule_event_id,
                    started_event_id: activity.started_event_id.unwrap_or(0),
                    identity: req.worker_identity.clone(),
                    // Carried from the caller's retry evaluation — the kernel
                    // does not re-derive it (`AddActivityTaskFailedEvent(...,
                    // retryState)` @ v1.31.0; kernel raise K1).
                    retry_state,
                    failure,
                });
            }
            ActivityResolution::TimedOut {
                timeout_type,
                retry_state,
                failure,
            } => {
                builder.emit(HistoryEventKind::ActivityTaskTimedOut {
                    activity_id: activity.activity_id.clone(),
                    scheduled_event_id: activity.schedule_event_id,
                    started_event_id: activity.started_event_id.unwrap_or(0),
                    timeout_type,
                    retry_state,
                    failure,
                });
            }
            ActivityResolution::Canceled { details } => {
                builder.emit(HistoryEventKind::ActivityTaskCanceled {
                    activity_id: activity.activity_id.clone(),
                    scheduled_event_id: activity.schedule_event_id,
                    started_event_id: activity.started_event_id.unwrap_or(0),
                    identity: req.worker_identity.clone(),
                    details,
                });
            }
        }

        builder.state.activities.remove(&activity.activity_id);
        builder.activity_ops.push(ActivityOp::Delete {
            activity_id: activity.activity_id,
        });

        if builder.state.pending_workflow_task.is_none() {
            builder.schedule_workflow_task();
        }

        Ok(builder.finish())
    }

    /// Record whether a child workflow started or failed to start.
    /// Validates `initiated_event_id` to guard against stale callbacks
    /// from a previous reset cycle.
    fn apply_child_start_confirmed(
        &self,
        loaded: LoadedRun,
        req: ChildStartConfirmedRequest,
    ) -> Result<Transition, Reject> {
        let state = expect_open(loaded)?;
        let mut builder = TransitionBuilder::new(state, req.now);
        let child = builder
            .state
            .children
            .get(&req.child_workflow_id)
            .cloned()
            .ok_or_else(|| Reject::UnknownChild(req.child_workflow_id.clone()))?;

        if child.initiated_event_id != req.initiated_event_id {
            return Err(Reject::StaleChildConfirmation {
                child_workflow_id: req.child_workflow_id,
                expected_initiated_event_id: child.initiated_event_id,
            });
        }

        match req.result {
            ChildStartResult::Started {
                child_run_id,
                workflow_type: _,
            } => {
                let child_run_id_for_state = child_run_id;
                let started_event_id =
                    builder.emit(HistoryEventKind::ChildWorkflowExecutionStarted {
                        child_workflow_id: child.child_workflow_id.clone(),
                        child_run_id,
                        workflow_type: child.workflow_type.clone(),
                        initiated_event_id: child.initiated_event_id,
                    });
                if let Some(current) = builder.state.children.get_mut(&child.child_workflow_id) {
                    current.child_run_id = Some(child_run_id_for_state);
                    current.started_event_id = Some(started_event_id);
                }
            }
            ChildStartResult::Failed { cause } => {
                builder.emit(HistoryEventKind::StartChildWorkflowExecutionFailed {
                    child_workflow_id: child.child_workflow_id.clone(),
                    initiated_event_id: child.initiated_event_id,
                    namespace_id: child.namespace_id,
                    namespace: child.namespace.clone(),
                    workflow_type: child.workflow_type.clone(),
                    cause,
                });
                builder.state.children.remove(&child.child_workflow_id);
            }
        }

        if builder.state.pending_workflow_task.is_none() {
            builder.schedule_workflow_task();
        }

        Ok(builder.finish())
    }

    /// Record the terminal outcome of a child workflow and remove it
    /// from the pending set.
    fn apply_child_resolved(
        &self,
        loaded: LoadedRun,
        req: ChildResolvedRequest,
    ) -> Result<Transition, Reject> {
        let state = expect_open(loaded)?;
        let mut builder = TransitionBuilder::new(state, req.now);
        let child = builder
            .state
            .children
            .get(&req.child_workflow_id)
            .cloned()
            .ok_or_else(|| Reject::UnknownChild(req.child_workflow_id.clone()))?;

        match req.resolution {
            ChildResolution::Completed { result } => {
                builder.emit(HistoryEventKind::ChildWorkflowExecutionCompleted {
                    child_workflow_id: child.child_workflow_id.clone(),
                    namespace_id: child.namespace_id,
                    namespace: child.namespace.clone(),
                    child_run_id: child.child_run_id,
                    workflow_type: child.workflow_type.clone(),
                    result,
                    initiated_event_id: child.initiated_event_id,
                    started_event_id: child.started_event_id.unwrap_or(0),
                });
            }
            ChildResolution::Failed { failure } => {
                builder.emit(HistoryEventKind::ChildWorkflowExecutionFailed {
                    child_workflow_id: child.child_workflow_id.clone(),
                    namespace_id: child.namespace_id,
                    namespace: child.namespace.clone(),
                    child_run_id: child.child_run_id,
                    workflow_type: child.workflow_type.clone(),
                    retry_state: RetryState::RetryPolicyNotSet,
                    failure,
                    initiated_event_id: child.initiated_event_id,
                    started_event_id: child.started_event_id.unwrap_or(0),
                });
            }
            ChildResolution::Canceled => {
                builder.emit(HistoryEventKind::ChildWorkflowExecutionCanceled {
                    child_workflow_id: child.child_workflow_id.clone(),
                    namespace_id: child.namespace_id,
                    namespace: child.namespace.clone(),
                    child_run_id: child.child_run_id,
                    workflow_type: child.workflow_type.clone(),
                    details: None,
                    initiated_event_id: child.initiated_event_id,
                    started_event_id: child.started_event_id.unwrap_or(0),
                });
            }
            ChildResolution::Terminated => {
                builder.emit(HistoryEventKind::ChildWorkflowExecutionTerminated {
                    child_workflow_id: child.child_workflow_id.clone(),
                    namespace_id: child.namespace_id,
                    namespace: child.namespace.clone(),
                    workflow_type: child.workflow_type.clone(),
                    initiated_event_id: child.initiated_event_id,
                    started_event_id: child.started_event_id.unwrap_or(0),
                });
            }
            ChildResolution::TimedOut => {
                builder.emit(HistoryEventKind::ChildWorkflowExecutionTimedOut {
                    child_workflow_id: child.child_workflow_id.clone(),
                    namespace_id: child.namespace_id,
                    namespace: child.namespace.clone(),
                    workflow_type: child.workflow_type.clone(),
                    retry_state: RetryState::Timeout,
                    initiated_event_id: child.initiated_event_id,
                    started_event_id: child.started_event_id.unwrap_or(0),
                });
            }
        }

        builder.state.children.remove(&child.child_workflow_id);

        if builder.state.pending_workflow_task.is_none() {
            builder.schedule_workflow_task();
        }

        Ok(builder.finish())
    }

    /// Record the outcome of a cross-workflow signal request.
    fn apply_external_signal_resolved(
        &self,
        loaded: LoadedRun,
        req: ExternalSignalResolvedRequest,
    ) -> Result<Transition, Reject> {
        let state = expect_open(loaded)?;
        let mut builder = TransitionBuilder::new(state, req.now);
        let pending = builder
            .state
            .pending_external_signals
            .get(&req.initiated_event_id)
            .cloned()
            .ok_or(Reject::UnknownExternalSignal(req.initiated_event_id))?;

        match req.result {
            ExternalSignalResult::Signaled => {
                builder.emit(HistoryEventKind::ExternalWorkflowExecutionSignaled {
                    initiated_event_id: pending.initiated_event_id,
                    namespace_id: pending.target_namespace_id,
                    namespace: pending.target_namespace.clone(),
                    target_workflow_id: pending.target_workflow_id,
                    target_run_id: pending.target_run_id,
                });
            }
            ExternalSignalResult::Failed { cause } => {
                builder.emit(HistoryEventKind::SignalExternalWorkflowExecutionFailed {
                    initiated_event_id: pending.initiated_event_id,
                    namespace_id: pending.target_namespace_id,
                    namespace: pending.target_namespace.clone(),
                    target_workflow_id: pending.target_workflow_id,
                    target_run_id: pending.target_run_id,
                    cause,
                });
            }
        }

        builder
            .state
            .pending_external_signals
            .remove(&req.initiated_event_id);

        if builder.state.pending_workflow_task.is_none() {
            builder.schedule_workflow_task();
        }

        Ok(builder.finish())
    }

    /// Record the outcome of a cross-workflow cancel request.
    fn apply_external_cancel_resolved(
        &self,
        loaded: LoadedRun,
        req: ExternalCancelResolvedRequest,
    ) -> Result<Transition, Reject> {
        let state = expect_open(loaded)?;
        let mut builder = TransitionBuilder::new(state, req.now);
        let pending = builder
            .state
            .pending_external_cancels
            .get(&req.initiated_event_id)
            .cloned()
            .ok_or(Reject::UnknownExternalCancel(req.initiated_event_id))?;

        match req.result {
            ExternalCancelResult::CancelRequested => {
                builder.emit(HistoryEventKind::ExternalWorkflowExecutionCancelRequested {
                    initiated_event_id: pending.initiated_event_id,
                    namespace_id: pending.target_namespace_id,
                    namespace: pending.target_namespace.clone(),
                    target_workflow_id: pending.target_workflow_id,
                    target_run_id: pending.target_run_id,
                });
            }
            ExternalCancelResult::Failed { cause } => {
                builder.emit(
                    HistoryEventKind::RequestCancelExternalWorkflowExecutionFailed {
                        initiated_event_id: pending.initiated_event_id,
                        namespace_id: pending.target_namespace_id,
                        namespace: pending.target_namespace.clone(),
                        target_workflow_id: pending.target_workflow_id,
                        target_run_id: pending.target_run_id,
                        cause,
                    },
                );
            }
        }

        builder
            .state
            .pending_external_cancels
            .remove(&req.initiated_event_id);

        if builder.state.pending_workflow_task.is_none() {
            builder.schedule_workflow_task();
        }

        Ok(builder.finish())
    }

    /// Record a Nexus operation lifecycle event (started, completed,
    /// failed, canceled, timed out). Only terminal resolutions remove
    /// the operation from the pending set and wake the workflow.
    fn apply_nexus_operation_resolved(
        &self,
        loaded: LoadedRun,
        req: NexusOperationResolvedRequest,
    ) -> Result<Transition, Reject> {
        let state = expect_open(loaded)?;
        let pending = state
            .pending_nexus_operations
            .get(&req.operation_id)
            .cloned()
            .ok_or_else(|| Reject::UnknownNexusOperation(req.operation_id.clone()))?;

        if pending.scheduled_event_id != req.scheduled_event_id {
            return Err(Reject::StaleNexusResolution {
                operation_id: req.operation_id,
                expected_scheduled_event_id: pending.scheduled_event_id,
            });
        }

        let mut builder = TransitionBuilder::new(state, req.now);
        match req.resolution {
            NexusResolution::Started {
                operation_token,
                links,
            } => {
                if pending.started {
                    return Err(Reject::NexusOperationAlreadyStarted(
                        pending.operation_id.clone(),
                    ));
                }
                builder.emit(HistoryEventKind::NexusOperationStarted {
                    operation_id: pending.operation_id.clone(),
                    scheduled_event_id: pending.scheduled_event_id,
                    operation_token: operation_token.clone(),
                    links,
                });
                if let Some(current) = builder
                    .state
                    .pending_nexus_operations
                    .get_mut(&pending.operation_id)
                {
                    current.started = true;
                    // Anchor start-to-close at acceptance time; the scanner reads
                    // this to fire start-to-close (`statemachine.go:159-167 @ v1.31.0`).
                    current.started_at = Some(builder.now);
                    // Retain the handler's async token so cancel/get re-send it verbatim
                    // (v1.31.0 keeps it in the NexusOperation mutable state, not only the
                    // event); the runtime cancel path reads it off this projection.
                    current.operation_token = operation_token;
                }
                // NexusOperationStarted is a workflow-task trigger
                // (`StartedEventDefinition.IsWorkflowTaskTrigger() -> true`,
                // components/nexusoperations/events.go @ v1.31.0): the async-started
                // transition must deliver a task so the worker observes the started
                // event, exactly like the terminal resolutions below.
                if builder.state.pending_workflow_task.is_none() {
                    builder.schedule_workflow_task();
                }
            }
            NexusResolution::Completed { result, links } => {
                builder.emit(HistoryEventKind::NexusOperationCompleted {
                    operation_id: pending.operation_id.clone(),
                    scheduled_event_id: pending.scheduled_event_id,
                    result,
                    links,
                });
                builder
                    .state
                    .pending_nexus_operations
                    .remove(&pending.operation_id);
                if builder.state.pending_workflow_task.is_none() {
                    builder.schedule_workflow_task();
                }
            }
            NexusResolution::Failed { failure } => {
                builder.emit(HistoryEventKind::NexusOperationFailed {
                    operation_id: pending.operation_id.clone(),
                    scheduled_event_id: pending.scheduled_event_id,
                    failure,
                });
                builder
                    .state
                    .pending_nexus_operations
                    .remove(&pending.operation_id);
                if builder.state.pending_workflow_task.is_none() {
                    builder.schedule_workflow_task();
                }
            }
            NexusResolution::AttemptFailed {
                failure,
                next_attempt_at,
            } => {
                // Retryable attempt failure: the operation stays pending and backs
                // off (v1.31.0 `SCHEDULED -> BACKING_OFF`, `statemachine.go:268-289`).
                // Record the handler failure + next-attempt time and bump the attempt
                // count, but emit NO history event and schedule NO workflow task — this
                // is internal backoff state the retry scanner reads, not a caller-visible
                // outcome (Invariant 1). `last_attempt_failure` is the handler's own
                // failure for Describe, NOT the terminal NexusOperationFailureInfo wrap.
                if let Some(current) = builder
                    .state
                    .pending_nexus_operations
                    .get_mut(&pending.operation_id)
                {
                    current.attempt = current.attempt.saturating_add(1);
                    current.last_attempt_failure = Some(failure);
                    current.next_attempt_at = Some(next_attempt_at);
                }
            }
            NexusResolution::Canceled => {
                builder.emit(HistoryEventKind::NexusOperationCanceled {
                    operation_id: pending.operation_id.clone(),
                    scheduled_event_id: pending.scheduled_event_id,
                    // Carry the operation identity so the edge wraps the canceled event in a
                    // NexusOperationFailureInfo (cause: CanceledFailureInfo) — without it the
                    // caller decodes no failure (`createNexusOperationFailure`,
                    // `completion.go:88-104 @ v1.31.0`).
                    endpoint: pending.endpoint.clone(),
                    service: pending.service.clone(),
                    operation: pending.operation.clone(),
                    operation_token: pending.operation_token.clone(),
                });
                builder
                    .state
                    .pending_nexus_operations
                    .remove(&pending.operation_id);
                if builder.state.pending_workflow_task.is_none() {
                    builder.schedule_workflow_task();
                }
            }
            NexusResolution::TimedOut { timeout_type } => {
                builder.emit(HistoryEventKind::NexusOperationTimedOut {
                    operation_id: pending.operation_id.clone(),
                    scheduled_event_id: pending.scheduled_event_id,
                    endpoint: pending.endpoint.clone(),
                    service: pending.service.clone(),
                    operation: pending.operation.clone(),
                    // v1.31.0 carries the async token only once started; tokeira
                    // uses the operation id as that token (`NexusResolution::Started`).
                    operation_token: if pending.started {
                        pending.operation_id.clone()
                    } else {
                        String::new()
                    },
                    timeout_type,
                });
                builder
                    .state
                    .pending_nexus_operations
                    .remove(&pending.operation_id);
                if builder.state.pending_workflow_task.is_none() {
                    builder.schedule_workflow_task();
                }
            }
        }

        Ok(builder.finish())
    }

    /// Re-dispatch a backing-off Nexus operation whose `next_attempt_at` has
    /// arrived (the retry scanner fired). Clears the backoff state (a fresh attempt
    /// is now in flight) and re-emits the `ScheduleNexusOperation` dispatch op from
    /// the pending op's retained fields — no history event (the original
    /// `NexusOperationScheduled` still stands, matching v1.31.0's
    /// `EventRescheduled`/InvocationTask which records none). Fenced: a stale
    /// scheduled-event id or an op that is not backing off (already re-dispatched by
    /// a concurrent tick, started, or resolved) is rejected, so there is no double
    /// re-dispatch.
    fn apply_nexus_operation_retry(
        &self,
        loaded: LoadedRun,
        req: NexusOperationRetryRequest,
    ) -> Result<Transition, Reject> {
        let state = expect_open(loaded)?;
        let pending = state
            .pending_nexus_operations
            .get(&req.operation_id)
            .cloned()
            .ok_or_else(|| Reject::UnknownNexusOperation(req.operation_id.clone()))?;
        if pending.scheduled_event_id != req.scheduled_event_id {
            return Err(Reject::StaleNexusResolution {
                operation_id: req.operation_id,
                expected_scheduled_event_id: pending.scheduled_event_id,
            });
        }
        if pending.next_attempt_at.is_none() {
            return Err(Reject::NexusOperationNotBackingOff(req.operation_id));
        }

        let run_key = state.run_key;
        let mut builder = TransitionBuilder::new(state, req.now);
        // Clear the backoff state (Invariant 4: cleared on re-dispatch, so Describe
        // shows nothing while the new attempt is in flight). The attempt count is not
        // bumped here — it was already incremented by the AttemptFailed that backed
        // the operation off.
        if let Some(current) = builder
            .state
            .pending_nexus_operations
            .get_mut(&pending.operation_id)
        {
            current.last_attempt_failure = None;
            current.next_attempt_at = None;
        }
        builder
            .dispatch_ops
            .push(DispatchOp::ScheduleNexusOperation {
                operation_id: pending.operation_id.clone(),
                endpoint: pending.endpoint.clone(),
                service: pending.service.clone(),
                operation: pending.operation.clone(),
                input: pending.input.clone(),
                schedule_to_close_timeout: pending.schedule_to_close_timeout,
                schedule_to_start_timeout: pending.schedule_to_start_timeout,
                start_to_close_timeout: pending.start_to_close_timeout,
                originator_run_key: run_key,
                scheduled_event_id: pending.scheduled_event_id,
                scheduled_at: pending.scheduled_at,
            });
        Ok(builder.finish())
    }

    /// Record a WFT failure. The pending WFT stays alive (with
    /// `started_event_id` cleared) so it can be retried — unlike
    /// completion, failure does not consume the logical task slot.
    fn apply_workflow_task_failed(
        &self,
        loaded: LoadedRun,
        req: WorkflowTaskFailedRequest,
    ) -> Result<Transition, Reject> {
        let state = expect_open(loaded)?;
        let pending = state
            .pending_workflow_task
            .clone()
            .ok_or(Reject::NoPendingWorkflowTask)?;
        let started_event_id = pending
            .started_event_id
            .ok_or(Reject::WorkflowTaskNotStarted {
                logical_seq: pending.logical_seq.0,
            })?;

        if pending.logical_seq != req.logical_seq {
            return Err(Reject::WorkflowTaskSeqMismatch {
                expected: pending.logical_seq.0,
                got: req.logical_seq.0,
            });
        }
        if started_event_id != req.started_event_id {
            return Err(Reject::WorkflowTaskTokenMismatch);
        }

        let mut builder = TransitionBuilder::new(state, req.now);
        // Only the attempt-1 failure persists a WorkflowTaskFailed event; a
        // transient (attempt>1) failure writes nothing — the retry chain lives
        // off-history ("Only emit WorkflowTaskFailedEvent if workflow task is
        // not transient", workflow_task_state_machine.go:892-895 @ v1.31.0;
        // spec transient-wft Req B.3).
        if pending.attempt == 1 {
            builder.emit(HistoryEventKind::WorkflowTaskFailed {
                logical_seq: pending.logical_seq,
                scheduled_event_id: pending.scheduled_event_id,
                started_event_id,
                failure_cause: req.failure_cause,
                failure_details: req.failure_details,
                identity: req.worker_identity,
                base_run_id: None,
                new_run_id: None,
                fork_event_version: None,
                fork_event_id: None,
            });
        }
        // Buffered events flush immediately after the WFT-failed event, before
        // the retry re-dispatch (`failWorkflowTask` fails then flushes,
        // `service/history/workflow/util.go:26 @ v1.31.0`).
        let flushed = builder.flush_buffered();
        let paused = builder.state.status == ExecutionStatus::Paused;
        if flushed > 0 && !paused {
            // Events buffered during the WFT invalidate any continuously-failing
            // (transient) suffix: v1.31.0 resets the next task to a NORMAL,
            // attempt-1 task with a REAL WorkflowTaskScheduled — the flushed events
            // must be delivered on a fresh, persisted task, not re-dispatched onto
            // the failed one (`workflow_task_state_machine.go:329-334 @ v1.31.0`;
            // spec transient-wft Req A.1, generalized by B.4 rule (i)).
            // `schedule_workflow_task` reads `workflow_task_attempt` for the emitted
            // event, so reset it to 1 first.
            builder.state.workflow_task_attempt = 1;
            builder.state.pending_workflow_task = None;
            builder.schedule_workflow_task();
        } else {
            // No new events (or paused): keep the failed task alive for a retry with
            // the same `logical_seq` and no new WorkflowTaskScheduled event. Phase B
            // refines this attempt>1 branch to virtual ids + event suppression.
            let sticky_preferred = builder
                .state
                .sticky
                .as_ref()
                .map(|sticky| sticky.worker_identity.clone());
            let virtual_scheduled_event_id = builder.state.last_event_id + 1;
            let current = builder
                .state
                .pending_workflow_task
                .as_mut()
                .expect("validated pending workflow task must still exist");
            current.started_event_id = None;
            current.started_at = None;
            builder.state.workflow_task_attempt += 1;
            current.attempt = builder.state.workflow_task_attempt;
            // The retry is transient: its Scheduled event is never persisted, so
            // its scheduled id becomes the VIRTUAL next-event id. This is what
            // lets `apply_workflow_task_started`'s transient branch fire (and its
            // conversion rule detect genuinely-new events) — keeping the stale
            // attempt-1 real id would misread every retry as "new events landed"
            // (`scheduledEventID = m.ms.GetNextEventID()`,
            // workflow_task_state_machine.go:376-379 @ v1.31.0; Req B.1/B.3).
            current.scheduled_event_id = virtual_scheduled_event_id;
            if !paused {
                builder.dispatch_ops.push(DispatchOp::EnqueueWorkflowTask {
                    queue: QueueKey {
                        namespace_id: builder.state.namespace_id,
                        task_queue: builder.state.task_queue.clone(),
                        task_kind: tokeira_types::TaskKind::Workflow,
                        deployment: builder.state.deployment.clone(),
                        build_id: builder.state.build_id.clone(),
                    },
                    logical_seq: pending.logical_seq,
                    sticky_preferred,
                });
            }
        }
        Ok(builder.finish())
    }

    /// Record a WFT timeout. Clears sticky affinity so the retry
    /// goes to the normal queue — the sticky worker is presumed dead.
    /// Clear the run's sticky affinity (`ResetStickyTaskQueue` →
    /// `ClearStickyTaskQueue` @ v1.31.0; sticky raise S5). Deliberately does
    /// NOT touch a pending sticky-dispatched WFT or its schedule-to-start
    /// deadline: the v1.31.0 timer keeps ticking after a reset and still
    /// fires (stickytq leaf 2). No history events.
    fn apply_reset_sticky(
        &self,
        loaded: LoadedRun,
        req: ResetStickyRequest,
    ) -> Result<Transition, Reject> {
        let state = expect_open(loaded)?;
        let mut builder = TransitionBuilder::new(state, req.now);
        builder.state.sticky = None;
        Ok(builder.finish())
    }

    fn apply_workflow_task_timed_out(
        &self,
        loaded: LoadedRun,
        req: WorkflowTaskTimedOutRequest,
    ) -> Result<Transition, Reject> {
        let state = expect_open(loaded)?;
        let pending = state
            .pending_workflow_task
            .clone()
            .ok_or(Reject::NoPendingWorkflowTask)?;

        // Sticky raise S3: schedule-to-start fires only on an UNSTARTED
        // sticky dispatch (the v1.31.0 timer guard no-ops once started,
        // timer_queue_active_task_executor.go:439-457) — a started or
        // superseded task makes the firing stale, surfaced as a reject the
        // scanner drops.
        if req.timeout_type == WorkflowTaskTimeoutType::ScheduleToStart {
            if pending.logical_seq != req.logical_seq {
                return Err(Reject::WorkflowTaskSeqMismatch {
                    expected: pending.logical_seq.0,
                    got: req.logical_seq.0,
                });
            }
            if pending.started_event_id.is_some() {
                return Err(Reject::WorkflowTaskTokenMismatch);
            }
            let mut builder = TransitionBuilder::new(state, req.now);
            // Always a REAL event: S2S deadlines exist only on sticky
            // attempt-1 dispatches (schedule_workflow_task guards them),
            // never on transient retries
            // (`AddWorkflowTaskScheduleToStartTimeoutEvent` emits
            // WorkflowTaskTimedOut(scheduled, EmptyEventID, SCHEDULE_TO_START),
            // workflow_task_state_machine.go:270-305 @ v1.31.0).
            builder.emit(HistoryEventKind::WorkflowTaskTimedOut {
                logical_seq: pending.logical_seq,
                scheduled_event_id: pending.scheduled_event_id,
                started_event_id: 0,
                timeout_type: WorkflowTaskTimeoutType::ScheduleToStart,
            });
            // `failWorkflowTask(incrementAttempt=false)`: clear sticky and do
            // NOT increment the attempt ("Do not increment workflow task
            // attempt in the case of sticky timeout to prevent creating next
            // workflow task as transient",
            // workflow_task_state_machine.go:263-268 @ v1.31.0) — the
            // reschedule is a real attempt-1 task routed to the NORMAL queue
            // now that the affinity is gone.
            builder.state.sticky = None;
            builder.state.workflow_task_attempt = 1;
            let _ = builder.flush_buffered();
            if builder.state.status == ExecutionStatus::Paused {
                let current = builder
                    .state
                    .pending_workflow_task
                    .as_mut()
                    .expect("validated pending workflow task must still exist");
                current.attempt = 1;
                // Disarm the fired deadline so a paused-retained task cannot
                // time out again before resume re-dispatches it.
                current.schedule_to_start_deadline = None;
            } else {
                builder.state.pending_workflow_task = None;
                builder.schedule_workflow_task();
            }
            return Ok(builder.finish());
        }

        let started_event_id = pending
            .started_event_id
            .ok_or(Reject::WorkflowTaskNotStarted {
                logical_seq: pending.logical_seq.0,
            })?;

        if pending.logical_seq != req.logical_seq {
            return Err(Reject::WorkflowTaskSeqMismatch {
                expected: pending.logical_seq.0,
                got: req.logical_seq.0,
            });
        }
        if started_event_id != req.started_event_id {
            return Err(Reject::WorkflowTaskTokenMismatch);
        }

        let mut builder = TransitionBuilder::new(state, req.now);
        // Only the attempt-1 timeout persists a WorkflowTaskTimedOut event; a
        // transient (attempt>1) timeout writes nothing
        // (workflow_task_state_machine.go:965-967 @ v1.31.0; Req B.3).
        if pending.attempt == 1 {
            builder.emit(HistoryEventKind::WorkflowTaskTimedOut {
                logical_seq: pending.logical_seq,
                scheduled_event_id: pending.scheduled_event_id,
                started_event_id,
                timeout_type: req.timeout_type,
            });
        }
        // WFT timeout is a close: buffered events flush after the timed-out
        // event and before the retry's fresh WorkflowTaskScheduled
        // (`AddWorkflowTaskScheduledEventAsHeartbeat` flushes first,
        // workflow_task_state_machine.go @ v1.31.0). As at WFT-failed close, a
        // non-empty flush converts the retry back to a NORMAL attempt-1 task
        // (rule (i), workflow_task_state_machine.go:329-338; Req B.4).
        let flushed = builder.flush_buffered();
        if flushed > 0 {
            builder.state.workflow_task_attempt = 1;
        } else {
            builder.state.workflow_task_attempt += 1;
        }
        builder.state.sticky = None;
        if builder.state.status == ExecutionStatus::Paused {
            // Paused workflows retain the pending task (cleared of started
            // state) so it can be re-dispatched when the workflow resumes.
            let current = builder
                .state
                .pending_workflow_task
                .as_mut()
                .expect("validated pending workflow task must still exist");
            current.started_event_id = None;
            current.started_at = None;
            current.attempt = builder.state.workflow_task_attempt;
        } else {
            // Active workflows get a fresh WorkflowTaskScheduled event so the
            // SDK state machine sees the correct Scheduled→Started sequence.
            builder.state.pending_workflow_task = None;
            builder.schedule_workflow_task();
        }
        Ok(builder.finish())
    }

    /// Fire a timer and wake the workflow. The timer is removed from
    /// the pending set because it is a one-shot construct.
    fn apply_timer_due(
        &self,
        loaded: LoadedRun,
        req: TimerDueRequest,
    ) -> Result<Transition, Reject> {
        let state = expect_open(loaded)?;
        let timer = state
            .timers
            .get(&req.timer_id)
            .cloned()
            .ok_or_else(|| Reject::UnknownTimer(req.timer_id.clone()))?;

        let mut builder = TransitionBuilder::new(state, req.fired_at);
        let fired = HistoryEventKind::TimerFired {
            timer_id: timer.timer_id.clone(),
            started_event_id: timer.started_event_id,
        };
        // A fire during a started WFT buffers (Phase 2 timer slice); the
        // WFT-close flush delivers it and schedules the follow-up task.
        if should_buffer(&builder.state, &fired) {
            builder.buffer(fired);
        } else {
            builder.emit(fired);
        }
        builder.state.timers.remove(&timer.timer_id);
        builder.timer_ops.push(TimerOp::Delete {
            timer_id: timer.timer_id,
        });

        if builder.state.pending_workflow_task.is_none() {
            builder.schedule_workflow_task();
        }

        Ok(builder.finish())
    }

    fn apply_workflow_start_delay_elapsed(
        &self,
        loaded: LoadedRun,
        req: WorkflowStartDelayElapsedRequest,
    ) -> Result<Transition, Reject> {
        let state = expect_open(loaded)?;
        let mut builder = TransitionBuilder::new(state, req.fired_at);
        if builder
            .state
            .timers
            .remove(WORKFLOW_START_DELAY_TIMER_ID)
            .is_none()
        {
            return Ok(builder.finish());
        }
        builder.timer_ops.push(TimerOp::Delete {
            timer_id: WORKFLOW_START_DELAY_TIMER_ID.to_string(),
        });

        if builder.state.pending_workflow_task.is_none() {
            builder.schedule_workflow_task();
        }

        Ok(builder.finish())
    }

    /// Ensure a WFT is pending so that an incoming query has a task
    /// to piggyback on. If a WFT is already in flight, the query will
    /// be delivered alongside it — no extra scheduling needed.
    fn apply_schedule_query_task(
        &self,
        loaded: LoadedRun,
        req: ScheduleQueryTaskRequest,
    ) -> Result<Transition, Reject> {
        let state = expect_open(loaded)?;

        // If a WFT is already pending, no-op — the query will be
        // piggybacked on the existing WFT when the worker polls.
        if state.pending_workflow_task.is_some() {
            let builder = TransitionBuilder::new(state, req.now);
            return Ok(builder.finish());
        }

        let mut builder = TransitionBuilder::new(state, req.now);
        builder.schedule_workflow_task();
        Ok(builder.finish())
    }

    /// Record the outcome of a completion-callback delivery attempt, advancing the
    /// callback's durable lifecycle.
    ///
    /// Unlike most apply methods this **accepts a closed run**: completion callbacks
    /// fire only after the workflow reaches a terminal state, so the lifecycle
    /// advance is a state-only commit on an already-closed run (only an absent run is
    /// rejected). It emits no history event — callback lifecycle is mutable state, not
    /// history — and no dispatch op; re-firing a `BackingOff` callback is the runtime
    /// scanner's job. The transition still bumps `transition_seq` so the durable
    /// callback state is fenced like any other commit. Mirrors the lifecycle the
    /// v1.31.0 callbacks component drives
    /// (`components/callbacks/statemachine.go @ v1.31.0`).
    fn apply_completion_callback_attempted(
        &self,
        loaded: LoadedRun,
        req: CompletionCallbackAttemptedRequest,
    ) -> Result<Transition, Reject> {
        let state = match loaded {
            LoadedRun::Absent => return Err(Reject::MissingRun),
            LoadedRun::Existing(state) => state,
        };

        // Fence: the callback must exist and must not already be terminal. A
        // terminal callback (`Succeeded`/`Failed`) is never re-attempted, so a late
        // attempt against one is rejected rather than silently overwriting it.
        if req.callback_index >= state.completion_callbacks.len() {
            return Err(Reject::UnknownCompletionCallback(req.callback_index));
        }
        if matches!(
            state.completion_callbacks[req.callback_index].state,
            crate::state::CallbackState::Succeeded | crate::state::CallbackState::Failed
        ) {
            return Err(Reject::CompletionCallbackAlreadyTerminal(
                req.callback_index,
            ));
        }

        let mut builder = TransitionBuilder::new(state, req.now);
        let callback = &mut builder.state.completion_callbacks[req.callback_index];
        match req.outcome {
            CallbackAttemptOutcome::Succeeded => {
                callback.state = crate::state::CallbackState::Succeeded;
                callback.next_attempt_at = None;
            }
            CallbackAttemptOutcome::RetryableFailure {
                failure,
                next_attempt_at,
            } => {
                callback.state = crate::state::CallbackState::BackingOff;
                callback.attempt += 1;
                callback.last_attempt_failure = Some(failure);
                callback.next_attempt_at = Some(next_attempt_at);
            }
            CallbackAttemptOutcome::NonRetryableFailure { failure } => {
                callback.state = crate::state::CallbackState::Failed;
                callback.last_attempt_failure = Some(failure);
                callback.next_attempt_at = None;
            }
        }
        Ok(builder.finish())
    }

    fn apply_replayed_event(&self, state: &mut WorkflowState, event: &HistoryEvent) {
        match &event.kind {
            HistoryEventKind::WorkflowExecutionStarted { .. } => {}
            HistoryEventKind::WorkflowExecutionSignaled { .. } => {}
            HistoryEventKind::WorkflowExecutionCancelRequested { .. } => {
                state.cancel_requested = true;
            }
            HistoryEventKind::WorkflowExecutionPaused {
                identity,
                reason,
                request_id,
            } => {
                state.status = ExecutionStatus::Paused;
                state.pause_info = Some(PauseInfo {
                    pause_time: event.happened_at,
                    identity: identity.clone(),
                    reason: reason.clone(),
                    request_id: request_id.clone(),
                });
            }
            HistoryEventKind::WorkflowExecutionUnpaused { .. } => {
                state.status = ExecutionStatus::Running;
                state.pause_info = None;
            }
            HistoryEventKind::WorkflowExecutionTerminated { .. } => {
                close_replayed_run(state, ExecutionStatus::Terminated, event.happened_at);
            }
            HistoryEventKind::WorkflowExecutionTimedOut { .. } => {
                close_replayed_run(state, ExecutionStatus::TimedOut, event.happened_at);
            }
            HistoryEventKind::WorkflowTaskScheduled {
                logical_seq,
                attempt,
                ..
            } => {
                state.timers.remove(WORKFLOW_START_DELAY_TIMER_ID);
                state.pending_workflow_task = Some(PendingWorkflowTask {
                    schedule_to_start_deadline: None,
                    logical_seq: *logical_seq,
                    scheduled_event_id: event.event_id,
                    scheduled_at: event.happened_at,
                    started_event_id: None,
                    started_at: None,
                    attempt: *attempt,
                });
                state.workflow_task_attempt = *attempt;
                if logical_seq.0 >= state.next_workflow_task_seq.0 {
                    state.next_workflow_task_seq = logical_seq.next();
                }
            }
            HistoryEventKind::WorkflowTaskStarted {
                logical_seq,
                scheduled_event_id,
                attempt,
                ..
            } => {
                state.pending_workflow_task = Some(PendingWorkflowTask {
                    schedule_to_start_deadline: None,
                    logical_seq: *logical_seq,
                    scheduled_event_id: *scheduled_event_id,
                    scheduled_at: state
                        .pending_workflow_task
                        .as_ref()
                        .map(|pending| pending.scheduled_at)
                        .unwrap_or(event.happened_at),
                    started_event_id: Some(event.event_id),
                    started_at: Some(event.happened_at),
                    attempt: *attempt,
                });
                if logical_seq.0 >= state.next_workflow_task_seq.0 {
                    state.next_workflow_task_seq = logical_seq.next();
                }
            }
            HistoryEventKind::WorkflowTaskCompleted {
                started_event_id,
                versioning_behavior,
                deployment_version,
                worker_deployment_name,
                ..
            } => {
                state.previous_started_event_id = *started_event_id;
                state.workflow_task_attempt = 1;
                state.pending_workflow_task = None;
                state.apply_wft_versioning(
                    *versioning_behavior,
                    deployment_version.clone(),
                    worker_deployment_name.clone(),
                );
            }
            HistoryEventKind::WorkflowTaskFailed {
                logical_seq,
                scheduled_event_id,
                failure_cause,
                ..
            } => {
                if *failure_cause == WorkflowTaskFailedCause::ResetWorkflow {
                    close_replayed_run(state, ExecutionStatus::Terminated, event.happened_at);
                } else {
                    let attempt = state
                        .pending_workflow_task
                        .as_ref()
                        .map(|pending| pending.attempt)
                        .unwrap_or(0);
                    state.pending_workflow_task = Some(PendingWorkflowTask {
                        schedule_to_start_deadline: None,
                        logical_seq: *logical_seq,
                        scheduled_event_id: *scheduled_event_id,
                        scheduled_at: state
                            .pending_workflow_task
                            .as_ref()
                            .map(|pending| pending.scheduled_at)
                            .unwrap_or(event.happened_at),
                        started_event_id: None,
                        started_at: None,
                        attempt,
                    });
                    state.workflow_task_attempt = attempt;
                    if logical_seq.0 >= state.next_workflow_task_seq.0 {
                        state.next_workflow_task_seq = logical_seq.next();
                    }
                }
            }
            HistoryEventKind::WorkflowTaskTimedOut {
                logical_seq,
                scheduled_event_id,
                ..
            } => {
                let attempt = state
                    .pending_workflow_task
                    .as_ref()
                    .map(|pending| pending.attempt)
                    .unwrap_or(0);
                state.pending_workflow_task = Some(PendingWorkflowTask {
                    schedule_to_start_deadline: None,
                    logical_seq: *logical_seq,
                    scheduled_event_id: *scheduled_event_id,
                    scheduled_at: state
                        .pending_workflow_task
                        .as_ref()
                        .map(|pending| pending.scheduled_at)
                        .unwrap_or(event.happened_at),
                    started_event_id: None,
                    started_at: None,
                    attempt,
                });
                state.workflow_task_attempt = attempt;
                state.sticky = None;
                if logical_seq.0 >= state.next_workflow_task_seq.0 {
                    state.next_workflow_task_seq = logical_seq.next();
                }
            }
            HistoryEventKind::ActivityTaskScheduled {
                activity_id,
                activity_type,
                task_queue,
                input,
                header,
                retry_policy,
                schedule_to_close_timeout,
                schedule_to_start_timeout,
                start_to_close_timeout,
                heartbeat_timeout,
                ..
            } => {
                state.activities.insert(
                    activity_id.clone(),
                    ActivityState {
                        cancel_requested: false,
                        started_identity: None,
                        retry_last_worker_identity: None,
                        activity_id: activity_id.clone(),
                        activity_type: activity_type.clone(),
                        schedule_event_id: event.event_id,
                        task_queue: task_queue.clone(),
                        deployment: state.deployment.clone(),
                        build_id: state.build_id.clone(),
                        input: input.clone(),
                        header: header.clone(),
                        attempt: 1,
                        retry_policy: retry_policy.clone(),
                        schedule_to_close_timeout: *schedule_to_close_timeout,
                        schedule_to_start_timeout: *schedule_to_start_timeout,
                        start_to_close_timeout: *start_to_close_timeout,
                        heartbeat_timeout: *heartbeat_timeout,
                        scheduled_at: event.happened_at,
                        current_attempt_scheduled_at: Some(event.happened_at),
                        started_at: None,
                        started_event_id: None,
                        last_failure: None,
                        heartbeat_details: None,
                        pause_info: None,
                        stamp: 0,
                    },
                );
            }
            HistoryEventKind::ActivityTaskStarted {
                activity_id,
                attempt,
                ..
            } => {
                if let Some(activity) = state.activities.get_mut(activity_id) {
                    activity.attempt = *attempt;
                    activity.started_at = Some(event.happened_at);
                    activity.started_event_id = Some(event.event_id);
                }
            }
            HistoryEventKind::ActivityTaskCompleted { activity_id, .. }
            | HistoryEventKind::ActivityTaskFailed { activity_id, .. }
            | HistoryEventKind::ActivityTaskTimedOut { activity_id, .. }
            | HistoryEventKind::ActivityTaskCanceled { activity_id, .. } => {
                state.activities.remove(activity_id);
            }
            HistoryEventKind::ActivityTaskCancelRequested { .. } => {}
            HistoryEventKind::TimerStarted {
                timer_id, fire_at, ..
            } => {
                state.timers.insert(
                    timer_id.clone(),
                    TimerState {
                        timer_id: timer_id.clone(),
                        started_event_id: event.event_id,
                        fire_at: *fire_at,
                    },
                );
            }
            HistoryEventKind::MarkerRecorded { .. } => {}
            HistoryEventKind::TimerCanceled { timer_id, .. }
            | HistoryEventKind::TimerFired { timer_id, .. } => {
                state.timers.remove(timer_id);
            }
            HistoryEventKind::StartChildWorkflowExecutionInitiated {
                child_workflow_id,
                namespace_id,
                namespace,
                workflow_type,
                parent_close_policy,
                ..
            } => {
                state.children.insert(
                    child_workflow_id.clone(),
                    ChildWorkflowState {
                        child_workflow_id: child_workflow_id.clone(),
                        namespace_id: *namespace_id,
                        namespace: namespace.clone(),
                        workflow_type: workflow_type.clone(),
                        child_run_id: None,
                        initiated_event_id: event.event_id,
                        started_event_id: None,
                        parent_close_policy: *parent_close_policy,
                    },
                );
            }
            HistoryEventKind::ChildWorkflowExecutionStarted {
                child_workflow_id,
                child_run_id,
                ..
            } => {
                if let Some(child) = state.children.get_mut(child_workflow_id) {
                    child.child_run_id = Some(*child_run_id);
                    child.started_event_id = Some(event.event_id);
                }
            }
            HistoryEventKind::StartChildWorkflowExecutionFailed {
                child_workflow_id, ..
            }
            | HistoryEventKind::ChildWorkflowExecutionCompleted {
                child_workflow_id, ..
            }
            | HistoryEventKind::ChildWorkflowExecutionFailed {
                child_workflow_id, ..
            }
            | HistoryEventKind::ChildWorkflowExecutionCanceled {
                child_workflow_id, ..
            }
            | HistoryEventKind::ChildWorkflowExecutionTerminated {
                child_workflow_id, ..
            }
            | HistoryEventKind::ChildWorkflowExecutionTimedOut {
                child_workflow_id, ..
            } => {
                state.children.remove(child_workflow_id);
            }
            HistoryEventKind::SignalExternalWorkflowExecutionInitiated {
                target_workflow_id,
                target_run_id,
                signal_name,
                namespace_id,
                namespace,
                ..
            } => {
                state.pending_external_signals.insert(
                    event.event_id,
                    PendingExternalSignal {
                        initiated_event_id: event.event_id,
                        target_namespace_id: *namespace_id,
                        target_namespace: namespace.clone(),
                        target_workflow_id: target_workflow_id.clone(),
                        target_run_id: *target_run_id,
                        signal_name: signal_name.clone(),
                    },
                );
            }
            HistoryEventKind::ExternalWorkflowExecutionSignaled {
                initiated_event_id, ..
            }
            | HistoryEventKind::SignalExternalWorkflowExecutionFailed {
                initiated_event_id, ..
            } => {
                state.pending_external_signals.remove(initiated_event_id);
            }
            HistoryEventKind::RequestCancelExternalWorkflowExecutionInitiated {
                target_workflow_id,
                target_run_id,
                namespace_id,
                namespace,
                ..
            } => {
                state.pending_external_cancels.insert(
                    event.event_id,
                    PendingExternalCancel {
                        initiated_event_id: event.event_id,
                        target_namespace_id: *namespace_id,
                        target_namespace: namespace.clone(),
                        target_workflow_id: target_workflow_id.clone(),
                        target_run_id: *target_run_id,
                    },
                );
            }
            HistoryEventKind::ExternalWorkflowExecutionCancelRequested {
                initiated_event_id,
                ..
            }
            | HistoryEventKind::RequestCancelExternalWorkflowExecutionFailed {
                initiated_event_id,
                ..
            } => {
                state.pending_external_cancels.remove(initiated_event_id);
            }
            HistoryEventKind::NexusOperationScheduled {
                operation_id,
                endpoint,
                service,
                operation,
                input,
                schedule_to_close_timeout,
                schedule_to_start_timeout,
                start_to_close_timeout,
                ..
            } => {
                state.pending_nexus_operations.insert(
                    operation_id.clone(),
                    PendingNexusOperation {
                        operation_id: operation_id.clone(),
                        scheduled_event_id: event.event_id,
                        endpoint: endpoint.clone(),
                        service: service.clone(),
                        operation: operation.clone(),
                        schedule_to_close_timeout: *schedule_to_close_timeout,
                        schedule_to_start_timeout: *schedule_to_start_timeout,
                        start_to_close_timeout: *start_to_close_timeout,
                        scheduled_at: event.happened_at,
                        started: false,
                        started_at: None,
                        // Backoff state is not carried on any history event
                        // (v1.31.0 records none for an attempt failure), so a
                        // replay-from-history op starts un-backed-off; the live
                        // persisted projection is the authority while running.
                        attempt: 0,
                        last_attempt_failure: None,
                        next_attempt_at: None,
                        operation_token: String::new(),
                        // Rebuilt from the scheduled event so a replayed op can still
                        // be re-dispatched on retry.
                        input: input.clone(),
                    },
                );
            }
            HistoryEventKind::NexusOperationStarted {
                operation_id,
                operation_token,
                ..
            } => {
                if let Some(operation) = state.pending_nexus_operations.get_mut(operation_id) {
                    operation.started = true;
                    // Anchor the start-to-close deadline at the started event's
                    // time so replay reconstructs the same deadline the live
                    // path set (`statemachine.go:159-167 @ v1.31.0`).
                    operation.started_at = Some(event.happened_at);
                    // Rebuild the handler async token from the event so a replayed op can
                    // still round-trip it on cancel.
                    operation.operation_token = operation_token.clone();
                }
            }
            HistoryEventKind::NexusOperationCompleted { operation_id, .. }
            | HistoryEventKind::NexusOperationFailed { operation_id, .. }
            | HistoryEventKind::NexusOperationCanceled { operation_id, .. }
            | HistoryEventKind::NexusOperationTimedOut { operation_id, .. } => {
                state.pending_nexus_operations.remove(operation_id);
            }
            HistoryEventKind::NexusOperationCancelRequested { .. } => {}
            HistoryEventKind::WorkflowExecutionUpdateAccepted {
                update_id,
                update_name,
                ..
            } => {
                state.pending_updates.insert(
                    update_id.clone(),
                    PendingUpdate {
                        update_id: update_id.clone(),
                        accepted_event_id: event.event_id,
                        name: update_name.clone(),
                    },
                );
            }
            HistoryEventKind::WorkflowExecutionUpdateCompleted { update_id, .. }
            | HistoryEventKind::WorkflowExecutionUpdateRejected { update_id, .. } => {
                state.pending_updates.remove(update_id);
            }
            HistoryEventKind::WorkflowExecutionOptionsUpdated {
                versioning_override,
                completion_callbacks,
                attached_completion_callbacks,
                attached_links,
                attached_request_id,
            } => {
                if let Some(attached_request_id) = attached_request_id {
                    // Reconstruct the attached request id → options-updated mapping
                    // on cold replay, matching the hot path in
                    // apply_update_execution_options (Req 5.3).
                    state.request_id_infos.insert(
                        attached_request_id.clone(),
                        RequestIdInfo {
                            event_id: event.event_id,
                            event_type: EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
                            buffered: false,
                        },
                    );
                }
                match versioning_override {
                    FieldChange::Set(value) => {
                        state.set_versioning_override(Some(value.clone()));
                    }
                    FieldChange::Clear => {
                        state.set_versioning_override(None);
                    }
                    FieldChange::Unchanged => {}
                }
                match completion_callbacks {
                    FieldChange::Set(value) => {
                        state.completion_callbacks = value.clone();
                    }
                    FieldChange::Clear => {
                        state.completion_callbacks.clear();
                    }
                    FieldChange::Unchanged => {}
                }
                state
                    .completion_callbacks
                    .extend(attached_completion_callbacks.clone());
                state.links.extend(attached_links.clone());
            }
            HistoryEventKind::WorkflowExecutionCompleted { result, .. } => {
                close_replayed_run(state, ExecutionStatus::Completed, event.happened_at);
                state.close_result = Some(result.clone());
            }
            HistoryEventKind::WorkflowExecutionFailed { failure, .. } => {
                close_replayed_run(state, ExecutionStatus::Failed, event.happened_at);
                state.close_failure = Some(failure.clone());
            }
            HistoryEventKind::WorkflowExecutionContinuedAsNew { .. } => {
                close_replayed_run(state, ExecutionStatus::ContinuedAsNew, event.happened_at);
            }
            HistoryEventKind::WorkflowExecutionCanceled { .. } => {
                close_replayed_run(state, ExecutionStatus::Cancelled, event.happened_at);
            }
        }
    }
}

fn close_replayed_run(
    state: &mut WorkflowState,
    status: ExecutionStatus,
    closed_at: OffsetDateTime,
) {
    state.status = status;
    state.closed_at = Some(closed_at);
    state.pending_workflow_task = None;
    state.sticky = None;
    state.pause_info = None;
    state.activities.clear();
    state.timers.clear();
    state.children.clear();
    state.pending_external_signals.clear();
    state.pending_external_cancels.clear();
    state.pending_updates.clear();
    state.pending_nexus_operations.clear();
}

/// Extract an open `WorkflowState` from a `LoadedRun`,
/// rejecting absent or closed runs.
fn initial_versioning_info(
    versioning_override: Option<VersioningOverride>,
) -> Option<WorkflowVersioningInfo> {
    versioning_override.map(|versioning_override| WorkflowVersioningInfo {
        versioning_override: Some(versioning_override),
        ..WorkflowVersioningInfo::default()
    })
}

fn expect_open(loaded: LoadedRun) -> Result<WorkflowState, Reject> {
    let state = match loaded {
        LoadedRun::Absent => return Err(Reject::MissingRun),
        LoadedRun::Existing(state) => state,
    };

    if !state.is_open() {
        return Err(Reject::RunClosed(state.status));
    }

    Ok(state)
}

/// Process a single workflow command produced by the worker during
/// WFT completion. Each command maps to one or more history events
/// and side-effect ops. Returns `true` if the command closed the
/// run, which tells the caller to reject any subsequent commands
/// in the batch.
fn emit_cron_continue_as_new(
    builder: &mut TransitionBuilder,
    workflow_task_completed_event_id: i64,
    cron: &CronContinuation,
    failure: Option<tokeira_types::Payload>,
    last_completion_result: Option<tokeira_types::Payloads>,
) {
    // Temporal creates cron successors through the continue-as-new path and
    // records the first-WFT backoff on that successor start
    // (`service/history/api/respondworkflowtaskcompleted/workflow_task_completed_handler.go:1383`,
    // `service/history/workflow/mutable_state_impl.go:2601 @ v1.31.0`).
    // Runtime computes the calendar delay; the kernel makes the successor
    // identity and delay authoritative by writing them into history.
    builder.emit(HistoryEventKind::WorkflowExecutionContinuedAsNew {
        workflow_task_completed_event_id,
        new_run_id: cron.new_run_id,
        workflow_type: builder.state.workflow_type.clone(),
        task_queue: builder.state.task_queue.clone(),
        input: cron.input.clone(),
        memo: builder.state.memo.clone(),
        search_attributes: builder.state.search_attributes.clone(),
        workflow_execution_timeout: builder.state.workflow_execution_timeout,
        workflow_run_timeout: builder.state.workflow_run_timeout,
        workflow_task_timeout: builder.state.workflow_task_timeout,
        retry_policy: builder.state.retry_policy.clone(),
        initiator: ContinueAsNewInitiator::CronSchedule,
        failure,
        last_completion_result,
        backoff_start_interval: Some(cron.first_workflow_task_backoff),
        cron_schedule: Some(cron.cron_schedule.clone()),
    });
    builder.close(ExecutionStatus::ContinuedAsNew);
}

fn apply_workflow_command(
    builder: &mut TransitionBuilder,
    command: WorkflowCommand,
    workflow_task_completed_event_id: i64,
    cron_continuation: Option<&CronContinuation>,
    retry_continuation: Option<&RetryContinuation>,
) -> Result<bool, Reject> {
    match command {
        WorkflowCommand::ScheduleActivity {
            activity_id,
            activity_type,
            task_queue,
            input,
            header,
            request_eager_execution: _,
            retry_policy,
            deployment,
            build_id,
            schedule_to_close_timeout,
            schedule_to_start_timeout,
            start_to_close_timeout,
            heartbeat_timeout,
        } => {
            if builder.state.activities.contains_key(&activity_id) {
                return Err(Reject::DuplicateActivityId(activity_id));
            }

            let schedule_event_id = builder.emit(HistoryEventKind::ActivityTaskScheduled {
                workflow_task_completed_event_id,
                activity_id: activity_id.clone(),
                activity_type: activity_type.clone(),
                task_queue: task_queue.clone(),
                input: input.clone(),
                header: header.clone(),
                retry_policy: retry_policy.clone(),
                schedule_to_close_timeout,
                schedule_to_start_timeout,
                start_to_close_timeout,
                heartbeat_timeout,
            });

            let activity = ActivityState {
                cancel_requested: false,
                started_identity: None,
                retry_last_worker_identity: None,
                activity_id: activity_id.clone(),
                activity_type,
                schedule_event_id,
                task_queue: task_queue.clone(),
                deployment: deployment
                    .clone()
                    .or_else(|| builder.state.deployment.clone()),
                build_id: build_id.clone().or_else(|| builder.state.build_id.clone()),
                input: input.clone(),
                header,
                attempt: 1,
                retry_policy: retry_policy.clone(),
                schedule_to_close_timeout,
                schedule_to_start_timeout,
                start_to_close_timeout,
                heartbeat_timeout,
                scheduled_at: builder.now,
                current_attempt_scheduled_at: Some(builder.now),
                started_at: None,
                started_event_id: None,
                last_failure: None,
                heartbeat_details: None,
                pause_info: None,
                stamp: 0,
            };
            builder
                .state
                .activities
                .insert(activity_id.clone(), activity.clone());
            builder
                .activity_ops
                .push(ActivityOp::Upsert(activity.clone()));
            builder.dispatch_ops.push(DispatchOp::EnqueueActivityTask {
                queue: QueueKey {
                    namespace_id: builder.state.namespace_id,
                    task_queue,
                    task_kind: tokeira_types::TaskKind::Activity,
                    deployment: activity.deployment.clone(),
                    build_id: activity.build_id.clone(),
                },
                activity_id,
                input,
                schedule_event_id,
                attempt: 1,
                dispatch_revision: builder
                    .state
                    .versioning_info
                    .as_ref()
                    .map(|info| info.revision_number)
                    .unwrap_or_default(),
                schedule_to_close_timeout,
                schedule_to_start_timeout,
                start_to_close_timeout,
                heartbeat_timeout,
            });
            Ok(false)
        }
        WorkflowCommand::StartTimer { timer_id, fire_at } => {
            if builder.state.timers.contains_key(&timer_id) {
                return Err(Reject::DuplicateTimerId(timer_id));
            }
            let started_event_id = builder.emit(HistoryEventKind::TimerStarted {
                workflow_task_completed_event_id,
                timer_id: timer_id.clone(),
                fire_at,
            });
            let timer = TimerState {
                timer_id: timer_id.clone(),
                started_event_id,
                fire_at,
            };
            builder.state.timers.insert(timer_id, timer.clone());
            builder.timer_ops.push(TimerOp::Upsert(timer));
            Ok(false)
        }
        WorkflowCommand::UpsertMemo(memo) => {
            builder.state.memo = memo.clone();
            builder.projection_ops.push(ProjectionOp::UpsertExecution {
                status: builder.state.status,
                memo_patch: memo,
                search_attr_patch: builder.state.search_attributes.clone(),
            });
            Ok(false)
        }
        WorkflowCommand::UpsertSearchAttributes(search_attributes) => {
            builder.state.search_attributes = search_attributes.clone();
            builder.projection_ops.push(ProjectionOp::UpsertExecution {
                status: builder.state.status,
                memo_patch: builder.state.memo.clone(),
                search_attr_patch: search_attributes,
            });
            Ok(false)
        }
        WorkflowCommand::RecordMarker {
            marker_name,
            details,
            failure,
            header,
        } => {
            // An empty marker name FAILS THE WORKFLOW TASK with
            // BadRecordMarkerAttributes (`ValidateRecordMarkerAttributes`,
            // command_attr_validator.go:194-211 @ v1.31.0); the corpus asserts
            // the rendered wire message verbatim
            // (tests/workflow_failures_test.go:373 @ v1.31.0).
            if marker_name.is_empty() {
                return Err(Reject::InvalidCommandAttributes {
                    cause: WorkflowTaskFailedCause::BadRecordMarkerAttributes,
                    message: Some("MarkerName is not set on RecordMarkerCommand.".to_string()),
                });
            }
            builder.emit(HistoryEventKind::MarkerRecorded {
                workflow_task_completed_event_id,
                marker_name,
                details,
                failure,
                header,
            });
            Ok(false)
        }
        WorkflowCommand::CompleteWorkflow { result } => {
            // A close command with buffered events is an UnhandledCommand:
            // the workflow must observe the buffered events before closing
            // (`hasBufferedEventsOrMessages` guards in the close handlers,
            // workflow_task_completed_handler.go @ v1.31.0). No causeErr —
            // the API layer renders the bare cause name, and the corpus
            // asserts err.Error() == "UnhandledCommand".
            if !builder.state.buffered_events.is_empty() {
                return Err(Reject::InvalidCommandAttributes {
                    cause: WorkflowTaskFailedCause::UnhandledCommand,
                    message: None,
                });
            }
            if let Some(cron) = cron_continuation {
                builder.state.close_result = Some(result.clone());
                emit_cron_continue_as_new(
                    builder,
                    workflow_task_completed_event_id,
                    cron,
                    None,
                    Some(result),
                );
            } else {
                builder.state.close_result = Some(result.clone());
                builder.emit(HistoryEventKind::WorkflowExecutionCompleted {
                    workflow_task_completed_event_id,
                    result,
                });
                builder.close(ExecutionStatus::Completed);
            }
            builder.apply_parent_close_policy();
            Ok(true)
        }
        WorkflowCommand::FailWorkflow { failure } => {
            // A close command with buffered events is an UnhandledCommand:
            // the workflow must observe the buffered events before closing
            // (`hasBufferedEventsOrMessages` guards in the close handlers,
            // workflow_task_completed_handler.go @ v1.31.0). No causeErr —
            // the API layer renders the bare cause name, and the corpus
            // asserts err.Error() == "UnhandledCommand".
            if !builder.state.buffered_events.is_empty() {
                return Err(Reject::InvalidCommandAttributes {
                    cause: WorkflowTaskFailedCause::UnhandledCommand,
                    message: None,
                });
            }
            builder.state.close_failure = Some(failure.clone());
            let attempt = builder.state.attempt;
            // Precedence is resolved in the runtime, which supplies at most one
            // of {retry_continuation, cron_continuation} for a FailWorkflow —
            // retry is evaluated before cron (`retry.go @ v1.31.0`), and the cron
            // helper already declines when a retry policy is present. The kernel
            // only records the already-decided outcome so replay reconstructs the
            // same close deterministically (retry evaluation is wall-clock- and
            // policy-dependent, hence a runtime concern, not pure kernel logic).
            if let Some(retry) = retry_continuation {
                // Binding new_execution_run_id to the retry outcome is what makes
                // a Failed close chase its successor: FixFollowEvents and the
                // runtime successor-start both key off this field (Req 1.2/2.2).
                let (retry_state, new_execution_run_id) = match retry {
                    RetryContinuation::Retry { new_run_id } => {
                        (RetryState::InProgress, Some(*new_run_id))
                    }
                    RetryContinuation::Terminal { retry_state } => (retry_state.clone(), None),
                };
                builder.emit(HistoryEventKind::WorkflowExecutionFailed {
                    workflow_task_completed_event_id,
                    failure,
                    retry_state,
                    attempt,
                    new_execution_run_id,
                });
                builder.close(ExecutionStatus::Failed);
            } else if let Some(cron) = cron_continuation {
                emit_cron_continue_as_new(
                    builder,
                    workflow_task_completed_event_id,
                    cron,
                    Some(failure),
                    None,
                );
            } else {
                // Terminal failure with neither retry nor cron. A run with a
                // retry policy but no runtime-supplied continuation is
                // unreachable once the runtime wires retry evaluation; the
                // conservative InProgress mapping is preserved for any caller
                // that omits the continuation rather than silently dropping it.
                let retry_state = if builder.state.retry_policy.is_some() {
                    RetryState::InProgress
                } else {
                    RetryState::RetryPolicyNotSet
                };
                builder.emit(HistoryEventKind::WorkflowExecutionFailed {
                    workflow_task_completed_event_id,
                    failure,
                    retry_state,
                    attempt,
                    new_execution_run_id: None,
                });
                builder.close(ExecutionStatus::Failed);
            }
            builder.apply_parent_close_policy();
            Ok(true)
        }
        WorkflowCommand::ContinueAsNew {
            new_run_id,
            workflow_type,
            task_queue,
            input,
            memo,
            search_attributes,
            workflow_execution_timeout,
            workflow_run_timeout,
            workflow_task_timeout,
            retry_policy,
        } => {
            // A close command with buffered events is an UnhandledCommand:
            // the workflow must observe the buffered events before closing
            // (`hasBufferedEventsOrMessages` guards in the close handlers,
            // workflow_task_completed_handler.go @ v1.31.0). No causeErr —
            // the API layer renders the bare cause name, and the corpus
            // asserts err.Error() == "UnhandledCommand".
            if !builder.state.buffered_events.is_empty() {
                return Err(Reject::InvalidCommandAttributes {
                    cause: WorkflowTaskFailedCause::UnhandledCommand,
                    message: None,
                });
            }
            builder.emit(HistoryEventKind::WorkflowExecutionContinuedAsNew {
                workflow_task_completed_event_id,
                new_run_id,
                workflow_type,
                task_queue,
                input,
                memo,
                search_attributes,
                workflow_execution_timeout,
                workflow_run_timeout,
                workflow_task_timeout,
                retry_policy: retry_policy
                    .clone()
                    .or_else(|| builder.state.retry_policy.clone()),
                initiator: ContinueAsNewInitiator::Workflow,
                failure: None,
                last_completion_result: None,
                backoff_start_interval: None,
                cron_schedule: None,
            });
            builder.close(ExecutionStatus::ContinuedAsNew);
            builder.apply_parent_close_policy();
            Ok(true)
        }
        WorkflowCommand::CancelWorkflow { details } => {
            // A close command with buffered events is an UnhandledCommand:
            // the workflow must observe the buffered events before closing
            // (`hasBufferedEventsOrMessages` guards in the close handlers,
            // workflow_task_completed_handler.go @ v1.31.0). No causeErr —
            // the API layer renders the bare cause name, and the corpus
            // asserts err.Error() == "UnhandledCommand".
            if !builder.state.buffered_events.is_empty() {
                return Err(Reject::InvalidCommandAttributes {
                    cause: WorkflowTaskFailedCause::UnhandledCommand,
                    message: None,
                });
            }
            builder.emit(HistoryEventKind::WorkflowExecutionCanceled {
                workflow_task_completed_event_id,
                details,
            });
            builder.close(ExecutionStatus::Cancelled);
            builder.apply_parent_close_policy();
            Ok(true)
        }
        WorkflowCommand::RequestCancelActivity { scheduled_event_id } => {
            // Keyed by scheduled_event_id — the proto command's only field
            // (kernel raise K4). Invalid requests FAIL THE WORKFLOW TASK
            // (`handleCommandRequestCancelActivity`,
            // workflow_task_completed_handler.go:626-668 @ v1.31.0), never
            // reject the completion.
            let target = builder
                .state
                .activities
                .values()
                .find(|activity| activity.schedule_event_id == scheduled_event_id)
                .cloned();
            let Some(activity) = target else {
                // The activity may have finished while this WFT ran — its
                // terminal event is buffered. v1.31.0 still records the
                // CancelRequested event and takes no further action
                // (`AddActivityTaskCancelRequestedEvent`,
                // mutable_state_impl.go:4364-4376 @ v1.31.0).
                if builder.state.buffered_events.iter().any(|buffered| {
                    activity_finish_scheduled_event_id(&buffered.kind) == Some(scheduled_event_id)
                }) {
                    builder.emit(HistoryEventKind::ActivityTaskCancelRequested {
                        workflow_task_completed_event_id,
                        activity_id: String::new(),
                        scheduled_event_id,
                    });
                    return Ok(false);
                }
                return Err(Reject::InvalidCommandAttributes {
                    cause: WorkflowTaskFailedCause::BadRequestCancelActivityAttributes,
                    message: Some(format!("ScheduledEventID: {scheduled_event_id}")),
                });
            };
            // Duplicate cancellation is a caller error too
            // (mutable_state_impl.go:4379-4388 @ v1.31.0).
            if activity.cancel_requested {
                return Err(Reject::InvalidCommandAttributes {
                    cause: WorkflowTaskFailedCause::BadRequestCancelActivityAttributes,
                    message: Some(format!("ScheduledEventID: {scheduled_event_id}")),
                });
            }
            builder.emit(HistoryEventKind::ActivityTaskCancelRequested {
                workflow_task_completed_event_id,
                activity_id: activity.activity_id.clone(),
                scheduled_event_id,
            });
            if activity.started_event_id.is_some() {
                // Started: set the durable cancel bit; the worker learns via
                // its next heartbeat response and resolves cooperatively
                // (`ai.CancelRequested = true` in
                // ApplyActivityTaskCancelRequestedEvent @ v1.31.0).
                let snapshot = {
                    let activity = builder
                        .state
                        .activities
                        .get_mut(&activity.activity_id)
                        .expect("activity present: found by scheduled_event_id above");
                    activity.cancel_requested = true;
                    activity.clone()
                };
                builder.activity_ops.push(ActivityOp::Upsert(snapshot));
            } else {
                // Not started: cancel immediately with the canonical details
                // payload and wake the workflow
                // (workflow_task_completed_handler.go:651-665; the constant
                // `activityCancellationMsgActivityNotStarted` at :48
                // @ v1.31.0).
                builder.emit(HistoryEventKind::ActivityTaskCanceled {
                    activity_id: activity.activity_id.clone(),
                    scheduled_event_id,
                    started_event_id: 0,
                    identity: None,
                    details: Some(activity_id_not_started_details()),
                });
                builder.state.activities.remove(&activity.activity_id);
                builder.activity_ops.push(ActivityOp::Delete {
                    activity_id: activity.activity_id.clone(),
                });
                if builder.state.is_open() && builder.state.pending_workflow_task.is_none() {
                    builder.schedule_workflow_task();
                }
            }
            Ok(false)
        }
        WorkflowCommand::CancelTimer { timer_id } => {
            if let Some(timer) = builder.state.timers.get(&timer_id) {
                let started_event_id = timer.started_event_id;
                builder.emit(HistoryEventKind::TimerCanceled {
                    workflow_task_completed_event_id,
                    timer_id: timer_id.clone(),
                    started_event_id,
                });
                builder.state.timers.remove(&timer_id);
                builder.timer_ops.push(TimerOp::Delete { timer_id });
                return Ok(false);
            }
            // The timer may have fired DURING this started WFT — its fired
            // event is buffered. v1.31.0 lets the cancel win: the buffered
            // TimerFired is deleted and only TimerCanceled reaches history
            // (`GetAndRemoveTimerFireEvent` in `AddTimerCanceledEvent`,
            // mutable_state_impl.go @ v1.31.0). The timer itself already left
            // durable state when it fired.
            if let Some(position) = builder.state.buffered_events.iter().position(|buffered| {
                matches!(
                    &buffered.kind,
                    HistoryEventKind::TimerFired { timer_id: fired, .. } if *fired == timer_id
                )
            }) {
                let removed = builder.state.buffered_events.remove(position);
                let HistoryEventKind::TimerFired {
                    started_event_id, ..
                } = removed.kind
                else {
                    unreachable!("position matched a TimerFired event");
                };
                builder.emit(HistoryEventKind::TimerCanceled {
                    workflow_task_completed_event_id,
                    timer_id: timer_id.clone(),
                    started_event_id,
                });
                return Ok(false);
            }
            // Neither running nor fired-and-buffered: v1.31.0 FAILS THE
            // WORKFLOW TASK (`failWorkflowTaskOnInvalidArgument(
            // BAD_CANCEL_TIMER_ATTRIBUTES)`), not the completion call — the
            // K4 invalid-command seam.
            Err(Reject::InvalidCommandAttributes {
                cause: WorkflowTaskFailedCause::BadCancelTimerAttributes,
                message: Some(format!("TimerID: {timer_id}")),
            })
        }
        WorkflowCommand::RequestNewWorkflowTask => {
            if builder.state.pending_workflow_task.is_none() && builder.state.is_open() {
                builder.schedule_workflow_task();
            }
            Ok(false)
        }
        WorkflowCommand::StartChildWorkflow {
            child_workflow_id,
            namespace_id,
            namespace,
            workflow_type,
            task_queue,
            input,
            header,
            memo,
            search_attributes,
            workflow_execution_timeout,
            workflow_run_timeout,
            workflow_task_timeout,
            retry_policy,
            cron_schedule,
            parent_close_policy,
        } => {
            if builder.state.children.contains_key(&child_workflow_id) {
                return Err(Reject::DuplicateChildWorkflowId(child_workflow_id));
            }
            // Inherit parent's task queue when the child's is empty,
            // matching Temporal's NormalizeAndValidateUserDefined behavior.
            let task_queue = if task_queue.0.is_empty() {
                builder.state.task_queue.clone()
            } else {
                task_queue
            };
            // Inherit parent's namespace when the child's is nil (empty
            // string from the SDK means "same namespace as parent").
            let namespace_id = if namespace_id.0.is_nil() {
                builder.state.namespace_id
            } else {
                namespace_id
            };
            let initiated_event_id =
                builder.emit(HistoryEventKind::StartChildWorkflowExecutionInitiated {
                    workflow_task_completed_event_id,
                    child_workflow_id: child_workflow_id.clone(),
                    workflow_type: workflow_type.clone(),
                    task_queue: task_queue.clone(),
                    input: input.clone(),
                    namespace_id,
                    namespace: namespace.clone(),
                    header,
                    memo,
                    search_attributes,
                    workflow_execution_timeout,
                    workflow_run_timeout,
                    workflow_task_timeout,
                    retry_policy,
                    cron_schedule,
                    parent_close_policy,
                });
            builder.state.children.insert(
                child_workflow_id.clone(),
                ChildWorkflowState {
                    child_workflow_id: child_workflow_id.clone(),
                    namespace_id,
                    namespace,
                    workflow_type: workflow_type.clone(),
                    child_run_id: None,
                    initiated_event_id,
                    started_event_id: None,
                    parent_close_policy,
                },
            );
            builder.dispatch_ops.push(DispatchOp::StartChildWorkflow {
                child_workflow_id,
                namespace_id,
                workflow_type,
                task_queue,
                input,
                parent_run_key: builder.state.run_key,
                parent_workflow_id: builder.state.workflow_id.clone(),
                parent_run_id: builder.state.run_id,
                parent_namespace_id: builder.state.namespace_id,
                parent_root_workflow_id: builder.state.root_workflow_id.clone(),
                parent_root_run_id: builder.state.root_run_id,
                initiated_event_id,
            });
            Ok(false)
        }
        WorkflowCommand::SignalExternalWorkflowExecution {
            target_namespace_id,
            target_namespace,
            target_workflow_id,
            target_run_id,
            signal_name,
            input,
            header,
            control,
        } => {
            let dispatch_header = header.clone();
            let initiated_event_id =
                builder.emit(HistoryEventKind::SignalExternalWorkflowExecutionInitiated {
                    workflow_task_completed_event_id,
                    namespace_id: target_namespace_id,
                    namespace: target_namespace.clone(),
                    target_workflow_id: target_workflow_id.clone(),
                    target_run_id,
                    signal_name: signal_name.clone(),
                    input: input.clone(),
                    header,
                    control: control.clone(),
                });
            builder.state.pending_external_signals.insert(
                initiated_event_id,
                PendingExternalSignal {
                    initiated_event_id,
                    target_namespace_id,
                    target_namespace,
                    target_workflow_id: target_workflow_id.clone(),
                    target_run_id,
                    signal_name: signal_name.clone(),
                },
            );
            builder
                .dispatch_ops
                .push(DispatchOp::SignalExternalWorkflow {
                    originator_run_key: builder.state.run_key,
                    namespace_id: target_namespace_id,
                    initiated_event_id,
                    target_workflow_id,
                    target_run_id,
                    signal_name,
                    input,
                    header: dispatch_header,
                });
            Ok(false)
        }
        WorkflowCommand::RequestCancelExternalWorkflowExecution {
            target_namespace_id,
            target_namespace,
            target_workflow_id,
            target_run_id,
            control,
        } => {
            // Attribute validation fails the WORKFLOW TASK with
            // BadRequestCancelExternalWorkflowExecutionAttributes and the
            // validator's message rendered from the RAW command attributes
            // (`ValidateCancelExternalWorkflowExecutionAttributes`,
            // command_attr_validator.go:250-293 @ v1.31.0).
            let raw_run_id = target_run_id.map(|r| r.0.to_string()).unwrap_or_default();
            let raw_namespace = target_namespace.clone().unwrap_or_default();
            // Inherit the source namespace when the command's is nil (empty
            // string from the SDK means "same namespace"), matching the
            // handler's default of the source namespace id
            // (`handleCommandRequestCancelExternalWorkflow`,
            // workflow_task_completed_handler.go:870-880 @ v1.31.0). Also
            // keeps the publisher's self-cancel guard honest for
            // namespace-less self-cancels.
            let target_namespace_id = if target_namespace_id.0.is_nil() {
                builder.state.namespace_id
            } else {
                target_namespace_id
            };
            if target_workflow_id.0.is_empty() {
                return Err(Reject::InvalidCommandAttributes {
                    cause:
                        WorkflowTaskFailedCause::BadRequestCancelExternalWorkflowExecutionAttributes,
                    message: Some(format!(
                        "WorkflowId is not set on RequestCancelExternalWorkflowExecutionCommand. \
                         Namespace={raw_namespace} RunId={raw_run_id}"
                    )),
                });
            }
            // Start + RequestCancel of the SAME child workflow id within one
            // completion batch is rejected before any event persists — the
            // check is against children initiated by EARLIER COMMANDS IN
            // THIS RespondWorkflowTaskCompleted only
            // (`initiatedChildExecutionsInBatch`, keyed by workflow id,
            // workflow_task_completed_handler.go:887-891,1150 @ v1.31.0).
            // Children from earlier workflow tasks have initiated event ids
            // below this completion's event id, so the id comparison
            // discriminates batch-local children exactly.
            if builder
                .state
                .children
                .get(&target_workflow_id)
                .is_some_and(|child| child.initiated_event_id > workflow_task_completed_event_id)
            {
                return Err(Reject::InvalidCommandAttributes {
                    cause:
                        WorkflowTaskFailedCause::BadRequestCancelExternalWorkflowExecutionAttributes,
                    message: Some(format!(
                        "Start and RequestCancel for child workflow is not allowed in same \
                         workflow task. WorkflowId={} RunId={raw_run_id} Namespace={raw_namespace}",
                        target_workflow_id.0
                    )),
                });
            }
            let initiated_event_id = builder.emit(
                HistoryEventKind::RequestCancelExternalWorkflowExecutionInitiated {
                    workflow_task_completed_event_id,
                    namespace_id: target_namespace_id,
                    namespace: target_namespace.clone(),
                    target_workflow_id: target_workflow_id.clone(),
                    target_run_id,
                    control: control.clone(),
                },
            );
            builder.state.pending_external_cancels.insert(
                initiated_event_id,
                PendingExternalCancel {
                    initiated_event_id,
                    target_namespace_id,
                    target_namespace,
                    target_workflow_id: target_workflow_id.clone(),
                    target_run_id,
                },
            );
            builder
                .dispatch_ops
                .push(DispatchOp::RequestCancelExternalWorkflow {
                    originator_run_key: builder.state.run_key,
                    originator_namespace_id: builder.state.namespace_id,
                    originator_workflow_id: builder.state.workflow_id.clone(),
                    originator_run_id: builder.state.run_id,
                    namespace_id: target_namespace_id,
                    initiated_event_id,
                    reason: format!(
                        "cancel requested by external workflow {}",
                        builder.state.workflow_id.0
                    ),
                    target_workflow_id,
                    target_run_id,
                });
            Ok(false)
        }
        WorkflowCommand::ScheduleNexusOperation {
            operation_id,
            endpoint,
            service,
            operation,
            input,
            schedule_to_close_timeout,
            schedule_to_start_timeout,
            start_to_close_timeout,
        } => {
            if builder
                .state
                .pending_nexus_operations
                .contains_key(&operation_id)
            {
                return Err(Reject::DuplicateNexusOperationId(operation_id));
            }
            let scheduled_event_id = builder.emit(HistoryEventKind::NexusOperationScheduled {
                workflow_task_completed_event_id,
                operation_id: operation_id.clone(),
                endpoint: endpoint.clone(),
                endpoint_id: endpoint.clone(),
                service: service.clone(),
                operation: operation.clone(),
                input: input.clone(),
                nexus_header: BTreeMap::new(),
                schedule_to_close_timeout,
                schedule_to_start_timeout,
                start_to_close_timeout,
            });
            builder.state.pending_nexus_operations.insert(
                operation_id.clone(),
                PendingNexusOperation {
                    operation_id: operation_id.clone(),
                    scheduled_event_id,
                    endpoint: endpoint.clone(),
                    service: service.clone(),
                    operation: operation.clone(),
                    schedule_to_close_timeout,
                    schedule_to_start_timeout,
                    start_to_close_timeout,
                    scheduled_at: builder.now,
                    started: false,
                    started_at: None,
                    attempt: 0,
                    last_attempt_failure: None,
                    next_attempt_at: None,
                    operation_token: String::new(),
                    input: input.clone(),
                },
            );
            builder
                .dispatch_ops
                .push(DispatchOp::ScheduleNexusOperation {
                    operation_id,
                    endpoint,
                    service,
                    operation,
                    input,
                    schedule_to_close_timeout,
                    schedule_to_start_timeout,
                    start_to_close_timeout,
                    originator_run_key: builder.state.run_key,
                    scheduled_event_id,
                    scheduled_at: builder.now,
                });
            Ok(false)
        }
        WorkflowCommand::CancelNexusOperation { scheduled_event_id } => {
            let pending = builder
                .state
                .pending_nexus_operations
                .values()
                .find(|pending| pending.scheduled_event_id == scheduled_event_id)
                .cloned()
                .ok_or_else(|| {
                    Reject::UnknownNexusOperation(format!(
                        "scheduled_event_id={scheduled_event_id}"
                    ))
                })?;
            builder.emit(HistoryEventKind::NexusOperationCancelRequested { scheduled_event_id });
            builder.dispatch_ops.push(DispatchOp::CancelNexusOperation {
                scheduled_event_id,
                originator_run_key: builder.state.run_key,
                operation_id: pending.operation_id,
                endpoint: pending.endpoint,
                service: pending.service,
            });
            Ok(false)
        }
        WorkflowCommand::UpdateCompleted { update_id, result } => {
            if !builder.state.pending_updates.contains_key(&update_id) {
                return Err(Reject::UnknownUpdate(update_id));
            }
            let accepted_event_id = builder
                .state
                .pending_updates
                .get(&update_id)
                .map(|update| update.accepted_event_id)
                .unwrap_or(0);
            builder.emit(HistoryEventKind::WorkflowExecutionUpdateCompleted {
                update_id: update_id.clone(),
                result,
                accepted_event_id,
            });
            builder.state.pending_updates.remove(&update_id);
            Ok(false)
        }
        WorkflowCommand::UpdateRejected { update_id, failure } => {
            // Same contract as the ProtocolMessage rejection body: no
            // durable trace (v1.31.0 writes no rejection event — the SDK
            // panics TMPRL1100 replaying one) and unknown ids are tolerated.
            let _ = failure;
            builder.state.admitted_updates.remove(&update_id);
            builder.state.pending_updates.remove(&update_id);
            Ok(false)
        }
        WorkflowCommand::ProtocolMessage {
            message_id: _,
            body,
        } => {
            match body {
                UpdateProtocolBody::Accepted {
                    update_id,
                    update_name,
                    input,
                } => {
                    if builder.state.pending_updates.contains_key(&update_id) {
                        return Err(Reject::DuplicateUpdateId(update_id));
                    }
                    // Move from admitted to pending on worker acceptance.
                    builder.state.admitted_updates.remove(&update_id);
                    let accepted_event_id =
                        builder.emit(HistoryEventKind::WorkflowExecutionUpdateAccepted {
                            update_id: update_id.clone(),
                            update_name: update_name.clone(),
                            input,
                            accepted_request_sequencing_event_id: 0,
                        });
                    builder.state.pending_updates.insert(
                        update_id.clone(),
                        PendingUpdate {
                            update_id,
                            accepted_event_id,
                            name: update_name,
                        },
                    );
                }
                UpdateProtocolBody::Completed { update_id, result } => {
                    if !builder.state.pending_updates.contains_key(&update_id) {
                        return Err(Reject::UnknownUpdate(update_id));
                    }
                    builder.emit(HistoryEventKind::WorkflowExecutionUpdateCompleted {
                        update_id: update_id.clone(),
                        result,
                        accepted_event_id: builder
                            .state
                            .pending_updates
                            .get(&update_id)
                            .map(|update| update.accepted_event_id)
                            .unwrap_or(0),
                    });
                    builder.state.pending_updates.remove(&update_id);
                }
                UpdateProtocolBody::Rejected { update_id, failure } => {
                    // A rejection leaves NO durable trace: v1.31.0 writes no
                    // history event ("RejectWorkflowExecutionUpdate is a
                    // no-op", mutable_state_impl.go; the SDK panics
                    // TMPRL1100-nondeterministic on replaying any rejection
                    // event) — the update simply vanishes from the admitted
                    // set and the rejection outcome reaches waiters through
                    // the runtime's transport resolution, off the event log.
                    // The same update id is re-admittable afterwards
                    // (rejected updates never count as completed,
                    // registry.go:496-506 @ v1.31.0). A rejection for an
                    // unknown or already-resolved update is tolerated as a
                    // no-op: v1.31.0 resurrects the update from the
                    // rejection's embedded request and resolves it without
                    // error (TryResurrect, registry.go:455-484), which is
                    // observably identical to ignoring it.
                    let _ = failure;
                    builder.state.admitted_updates.remove(&update_id);
                    builder.state.pending_updates.remove(&update_id);
                }
            }
            Ok(false)
        }
    }
}

/// Internal builder that assembles a [`Transition`] from a
/// sequence of mutations.
///
/// Takes ownership of the current `WorkflowState`, provides
/// helpers for emitting events, scheduling workflow tasks,
/// and closing the run, then produces the final `Transition`
/// on `finish()`.
///
/// Every `apply_*` method creates exactly one builder, so
/// each transition is an atomic unit: either all mutations
/// commit together or none do. The builder also captures
/// `expected_seq` at construction time to serve as the
/// optimistic concurrency fence when the runtime persists
/// the transition.
///
/// See `docs/architecture/020-kernel.md` §Transition builder.
/// Whether `kind` must be held in `buffered_events` rather than appended,
/// given the run's current WFT state. Mirrors the v1.31.0 `bufferEvent`
/// predicate (`service/history/historybuilder/event_store.go:263 @ v1.31.0`):
/// workflow state-change events, workflow-task events, and events generated
/// directly from a worker command or protocol message are never buffered;
/// other externally-originated events buffer while a WFT is *started* (a
/// worker holds it and its history view is frozen). Phase 1 scopes the
/// bufferable set to `WorkflowExecutionSignaled` and
/// `WorkflowExecutionCancelRequested`; Phase 2 extends it to
/// activity/child/Nexus completion-class events per the full predicate
/// (spec: kernel-event-buffering Req 2.1).
fn should_buffer(state: &WorkflowState, kind: &HistoryEventKind) -> bool {
    let workflow_task_started = state
        .pending_workflow_task
        .as_ref()
        .is_some_and(|pending| pending.started_event_id.is_some());
    if !workflow_task_started {
        return false;
    }
    matches!(
        kind,
        HistoryEventKind::WorkflowExecutionSignaled { .. }
            | HistoryEventKind::WorkflowExecutionCancelRequested { .. }
            // Phase 2 timer slice (spec kernel-event-buffering; trigger met by
            // TestCancelTimer_CancelFiredAndBuffered): a timer firing during a
            // started WFT buffers, so a CancelTimer in that WFT's completion
            // can still win by deleting the buffered fired event
            // (`GetAndRemoveTimerFireEvent` @ v1.31.0).
            | HistoryEventKind::TimerFired { .. }
    )
}

struct TransitionBuilder {
    /// Mutable working copy of the run state.
    state: WorkflowState,
    /// Wall-clock time for all events in this transition.
    now: OffsetDateTime,
    history_events: SmallVec<[HistoryEvent; 8]>,
    request_dedupe_ops: SmallVec<[RequestDedupeOp; 1]>,
    activity_ops: SmallVec<[ActivityOp; 4]>,
    timer_ops: SmallVec<[TimerOp; 4]>,
    dispatch_ops: SmallVec<[DispatchOp; 4]>,
    projection_ops: SmallVec<[ProjectionOp; 8]>,
    /// The `TransitionSeq` captured at construction time,
    /// used as the optimistic concurrency fence.
    expected_seq: TransitionSeq,
}

impl TransitionBuilder {
    /// Create a new builder from the current state and a
    /// wall-clock timestamp.
    fn new(state: WorkflowState, now: OffsetDateTime) -> Self {
        let expected_seq = state.transition_seq;
        Self {
            state,
            now,
            history_events: SmallVec::new(),
            request_dedupe_ops: SmallVec::new(),
            activity_ops: SmallVec::new(),
            timer_ops: SmallVec::new(),
            dispatch_ops: SmallVec::new(),
            projection_ops: SmallVec::new(),
            expected_seq,
        }
    }

    /// Append a history event and return its assigned event
    /// ID. Event IDs are contiguous within a transition.
    fn emit(&mut self, kind: HistoryEventKind) -> i64 {
        self.emit_at(self.now, kind)
    }

    /// [`Self::emit`] with an explicit timestamp, for events whose recorded
    /// time predates the transition applying them — e.g. a transient WFT's
    /// late-materialized Scheduled/Started pair carries the task's actual
    /// schedule/start times, not the completion time
    /// (`workflowTask.ScheduledTime` / `workflowTask.StartedTime` in
    /// `AddWorkflowTaskCompletedEvent`,
    /// workflow_task_state_machine.go:768-800 @ v1.31.0).
    fn emit_at(&mut self, happened_at: OffsetDateTime, kind: HistoryEventKind) -> i64 {
        let event_id = self.state.last_event_id + 1;
        self.state.last_event_id = event_id;
        self.history_events.push(HistoryEvent {
            event_id,
            happened_at,
            kind,
        });
        event_id
    }

    /// Hold an event on durable state instead of appending it: no event id is
    /// consumed and `last_event_id` is untouched until the WFT closes and
    /// [`Self::flush_buffered`] emits it (spec: kernel-event-buffering
    /// Req 2.1, 6.1.1).
    fn buffer(&mut self, kind: HistoryEventKind) {
        self.state
            .buffered_events
            .push(crate::state::BufferedEvent {
                admitted_at: self.now,
                kind,
            });
    }

    /// Drain `buffered_events` into history in admission order, assigning
    /// contiguous event ids continuing from the last emitted event, and
    /// return how many events were flushed. Called at every WFT-close site
    /// (completed / failed / timed-out / forced close). Flushed events keep
    /// their admission timestamps (v1.31.0 buffered events are fully formed
    /// at admission except the id; `EventStore::add`, event_store.go:74).
    /// Phase 1 has no completion-class buffered events, so admission order is
    /// the final order (`reorderBuffer` is Phase 2; event_store.go:411).
    fn flush_buffered(&mut self) -> usize {
        let buffered = std::mem::take(&mut self.state.buffered_events);
        let count = buffered.len();
        for event in buffered {
            let event_id = self.state.last_event_id + 1;
            self.state.last_event_id = event_id;
            self.history_events.push(HistoryEvent {
                event_id,
                happened_at: event.admitted_at,
                kind: event.kind,
            });
        }
        count
    }

    /// Fail a started WFT with cause `ForceCloseCommand` as the first step of
    /// a forced workflow close (terminate / workflow timeout), so the close
    /// batch's first event is the resulting `WorkflowTaskFailed`
    /// (`failWorkflowTask` via `TerminateWorkflow`/`TimeoutWorkflow`,
    /// `service/history/workflow/util.go:26,71,115 @ v1.31.0`). No-op when no
    /// WFT is started. The identity is the internal history-service identity
    /// (`consts.IdentityHistoryService`, const.go:13 @ v1.31.0).
    fn force_close_started_workflow_task(&mut self) {
        let Some(pending) = self.state.pending_workflow_task.clone() else {
            return;
        };
        let Some(started_event_id) = pending.started_event_id else {
            return;
        };
        // Force-closing a TRANSIENT (attempt>1) task writes nothing — its
        // Scheduled/Started were never persisted, so there is no event to fail
        // ("Failing transient WT doesn't create any events at all and
        // wtFailedEvent is nil", workflow/util.go:118-120 @ v1.31.0; Req B.5).
        // The close batch's first event is then the terminal event itself.
        if pending.attempt > 1 {
            self.state.pending_workflow_task = None;
            return;
        }
        self.emit(HistoryEventKind::WorkflowTaskFailed {
            logical_seq: pending.logical_seq,
            scheduled_event_id: pending.scheduled_event_id,
            started_event_id,
            failure_cause: WorkflowTaskFailedCause::ForceCloseCommand,
            failure_details: None,
            identity: WorkerIdentity("history-service".to_string()),
            base_run_id: None,
            new_run_id: None,
            fork_event_version: None,
            fork_event_id: None,
        });
        // The force-failed task is not retried — the run is about to close.
        self.state.pending_workflow_task = None;
    }

    /// Shared terminate tail: force-close a started WFT, flush buffered
    /// events, emit `WorkflowExecutionTerminated`, close as `Terminated`, and
    /// clean up activities/timers/children. Ordering per `TerminateWorkflow`
    /// (util.go:115 @ v1.31.0): WorkflowTaskFailed(ForceCloseCommand) →
    /// flushed buffered events → WorkflowExecutionTerminated.
    fn terminate_run(&mut self, reason: String, details: Option<Payloads>, identity: String) {
        self.force_close_started_workflow_task();
        self.flush_buffered();
        self.emit(HistoryEventKind::WorkflowExecutionTerminated {
            reason,
            details,
            identity,
        });
        self.close(ExecutionStatus::Terminated);

        let activities = std::mem::take(&mut self.state.activities);
        for (activity_id, _) in activities {
            self.activity_ops.push(ActivityOp::Delete { activity_id });
        }

        let timers = std::mem::take(&mut self.state.timers);
        for (timer_id, _) in timers {
            self.timer_ops.push(TimerOp::Delete { timer_id });
        }

        self.apply_parent_close_policy();
    }

    /// Schedule a workflow task: emit the scheduled event,
    /// set the pending WFT on state, and push a dispatch op.
    /// No-ops if the workflow is paused.
    fn schedule_workflow_task(&mut self) {
        if self.state.status == ExecutionStatus::Paused {
            return;
        }
        let logical_seq = self.state.next_workflow_task_seq;
        self.state.next_workflow_task_seq = logical_seq.next();
        // A continuously-failing workflow task (attempt > 1) is *transient*: v1.31.0
        // does not persist its WorkflowTaskScheduled event. The scheduled id is the
        // virtual next-event id (last_event_id + 1) and `last_event_id` is NOT
        // advanced; the event materializes only if the task later succeeds (see
        // apply_workflow_task_completed) or is converted back to normal at start time
        // when new events arrived (`IsTransientWorkflowTask`, mutable_state_impl.go:2250;
        // `createWorkflowTaskScheduledEvent = !IsTransientWorkflowTask()`,
        // workflow_task_state_machine.go:326,368-379 @ v1.31.0). Buffered events at
        // scheduling reset to a normal attempt-1 task before reaching here (the
        // flush-reset branch in apply_workflow_task_failed / _timed_out).
        // Sticky raise S2: while a sticky affinity (with a queue) holds, the
        // next WFT is scheduled ON the sticky queue with a per-dispatch
        // schedule-to-start deadline (the scheduled event records the sticky
        // queue, mirroring `CurrentTaskQueue()` @ v1.31.0). Guarded to
        // attempt-1: v1.31.0 clears sticky on any WFT failure before a
        // transient retry could exist (S4, deferred), and pre-S1 encodings
        // (empty queue name) degrade to a normal-queue sync-match hint.
        let sticky_dispatch = self
            .state
            .sticky
            .as_ref()
            .filter(|sticky| {
                self.state.workflow_task_attempt == 1 && !sticky.sticky_queue.0.is_empty()
            })
            .cloned();
        let dispatch_task_queue = sticky_dispatch
            .as_ref()
            .map(|sticky| sticky.sticky_queue.clone())
            .unwrap_or_else(|| self.state.task_queue.clone());
        let schedule_to_start_deadline = sticky_dispatch
            .as_ref()
            .map(|sticky| self.now + sticky.schedule_to_start_timeout);
        let scheduled_event_id = if self.state.workflow_task_attempt > 1 {
            self.state.last_event_id + 1
        } else {
            self.emit(HistoryEventKind::WorkflowTaskScheduled {
                logical_seq,
                task_queue: dispatch_task_queue.clone(),
                workflow_task_timeout: self.state.workflow_task_timeout,
                attempt: self.state.workflow_task_attempt,
            })
        };
        self.state.pending_workflow_task = Some(PendingWorkflowTask {
            logical_seq,
            scheduled_event_id,
            scheduled_at: self.now,
            started_event_id: None,
            started_at: None,
            attempt: self.state.workflow_task_attempt,
            schedule_to_start_deadline,
        });
        self.dispatch_ops.push(DispatchOp::EnqueueWorkflowTask {
            queue: QueueKey {
                namespace_id: self.state.namespace_id,
                task_queue: dispatch_task_queue,
                task_kind: tokeira_types::TaskKind::Workflow,
                deployment: self.state.deployment.clone(),
                build_id: self.state.build_id.clone(),
            },
            logical_seq,
            sticky_preferred: self
                .state
                .sticky
                .as_ref()
                .map(|s| s.worker_identity.clone()),
        });
    }

    /// Start the just-scheduled WFT in the same transition for runtime-owned
    /// sync-match. The runtime strips the enqueue op and delivers directly to
    /// the reserved poller after the commit succeeds.
    fn start_pending_workflow_task(&mut self, identity: WorkerIdentity) {
        let Some(pending) = self.state.pending_workflow_task.clone() else {
            return;
        };
        if pending.started_event_id.is_some() {
            return;
        }
        let attempt = pending.attempt.max(1);
        let started_event_id = self.emit(HistoryEventKind::WorkflowTaskStarted {
            logical_seq: pending.logical_seq,
            scheduled_event_id: pending.scheduled_event_id,
            attempt,
            identity,
            request_id: format!("sync-match-{}", pending.logical_seq.0),
            history_size_bytes: self.state.last_event_id,
            suggest_continue_as_new: false,
        });
        let current = self
            .state
            .pending_workflow_task
            .as_mut()
            .expect("pending workflow task was just observed");
        current.started_event_id = Some(started_event_id);
        current.started_at = Some(self.now);
        current.attempt = attempt;
    }

    /// Transition the run to a terminal status.
    ///
    /// Beyond setting the status, this clears all pending subsystem
    /// state (WFT, sticky, pause, external signals/cancels, updates,
    /// nexus ops) because none of those can ever resolve once the run
    /// is closed. Cleaning them here keeps the persisted state minimal
    /// and prevents stale callbacks from matching.
    fn close(&mut self, status: ExecutionStatus) {
        self.state.status = status;
        self.state.closed_at = Some(self.now);
        self.schedule_completion_callbacks();
        self.state.pending_workflow_task = None;
        self.state.sticky = None;
        self.state.pause_info = None;
        self.state.pending_external_signals.clear();
        self.state.pending_external_cancels.clear();
        self.state.pending_updates.clear();
        self.state.admitted_updates.clear();
        self.state.pending_nexus_operations.clear();
        // Buffered events that survive to a close are dropped, not flushed:
        // once the workflow finished they can never be delivered, and v1.31.0
        // discards them the same way (`FlushBufferToCurrentBatch`
        // workflowFinished branch, event_store.go:139 @ v1.31.0). Forced
        // closes (terminate / workflow timeout) flush *before* their terminal
        // event, so this only drops events overtaken by a worker close
        // command. Keeps closed runs buffer-free (Req 6.3).
        self.state.buffered_events.clear();
        self.projection_ops.push(ProjectionOp::CloseExecution {
            status,
            closed_at: self.now,
        });
    }

    fn schedule_completion_callbacks(&mut self) {
        // The completion event is the last event emitted in this closing transition
        // (close() runs immediately after the terminal event). Derive the outcome
        // once and convey it on every fired callback, mirroring v1.31.0's
        // GetNexusCompletion reading the completion event (mutable_state_impl.go @ v1.31.0).
        let outcome = self
            .history_events
            .last()
            .and_then(|event| callback_completion_outcome(&event.kind));
        for (callback_index, callback) in self.state.completion_callbacks.iter_mut().enumerate() {
            if callback.trigger != crate::state::CallbackTrigger::WorkflowClosed
                || callback.state != crate::state::CallbackState::Standby
            {
                continue;
            }
            // Defensive: a WorkflowClosed callback can only fire from a terminal
            // event we can map. If the close path ever emits an unmapped final
            // event, leave the callback Standby rather than dispatch without an
            // outcome (it cannot be delivered meaningfully).
            let Some(outcome) = outcome.clone() else {
                continue;
            };

            callback.state = crate::state::CallbackState::Scheduled;
            self.dispatch_ops
                .push(DispatchOp::DispatchCompletionCallback {
                    callback_index,
                    callback: callback.clone(),
                    outcome,
                });
        }
    }

    /// Apply parent close policy to all open child workflows.
    /// Emits `TerminateChild` or `CancelChild` dispatch ops
    /// as appropriate; `Abandon` children are left alone.
    fn apply_parent_close_policy(&mut self) {
        let children = std::mem::take(&mut self.state.children);
        for (_, child) in children {
            let Some(child_run_id) = child.child_run_id else {
                continue;
            };
            match child.parent_close_policy {
                ParentClosePolicy::Terminate => {
                    self.dispatch_ops.push(DispatchOp::TerminateChild {
                        namespace_id: child.namespace_id,
                        child_workflow_id: child.child_workflow_id,
                        child_run_id,
                        reason: "parent workflow closed".into(),
                    });
                }
                ParentClosePolicy::RequestCancel => {
                    self.dispatch_ops.push(DispatchOp::CancelChild {
                        namespace_id: child.namespace_id,
                        child_workflow_id: child.child_workflow_id,
                        child_run_id,
                        reason: "parent workflow closed".into(),
                    });
                }
                ParentClosePolicy::Abandon => {}
            }
        }
    }

    /// Consume the builder and produce the final
    /// [`Transition`]. Increments `transition_seq` exactly
    /// once.
    fn finish(mut self) -> Transition {
        self.state.transition_seq = self.state.transition_seq.next();
        Transition {
            expected_seq: self.expected_seq,
            next_state: self.state,
            history_events: self.history_events,
            request_dedupe_ops: self.request_dedupe_ops,
            activity_ops: self.activity_ops,
            timer_ops: self.timer_ops,
            dispatch_ops: self.dispatch_ops,
            projection_ops: self.projection_ops,
        }
    }
}

/// The `ActivityTaskScheduled` event id a buffered terminal activity event
/// resolves, if `kind` is one (used by the cancel command's already-finished
/// tolerance, `hBuilder.HasActivityFinishEvent` @ v1.31.0).
fn activity_finish_scheduled_event_id(kind: &HistoryEventKind) -> Option<i64> {
    match kind {
        HistoryEventKind::ActivityTaskCompleted {
            scheduled_event_id, ..
        }
        | HistoryEventKind::ActivityTaskFailed {
            scheduled_event_id, ..
        }
        | HistoryEventKind::ActivityTaskTimedOut {
            scheduled_event_id, ..
        }
        | HistoryEventKind::ActivityTaskCanceled {
            scheduled_event_id, ..
        } => Some(*scheduled_event_id),
        _ => None,
    }
}

/// The canonical details payload for an immediately-cancelled unstarted
/// activity: `payloads.EncodeString("ACTIVITY_ID_NOT_STARTED")` — a single
/// json/plain payload (`activityCancellationMsgActivityNotStarted`,
/// workflow_task_completed_handler.go:48,659 @ v1.31.0).
fn activity_id_not_started_details() -> Payloads {
    let mut metadata = std::collections::BTreeMap::new();
    metadata.insert("encoding".to_string(), "json/plain".to_string());
    Payloads(vec![tokeira_types::Payload {
        data: b"\"ACTIVITY_ID_NOT_STARTED\"".to_vec(),
        metadata,
        external_payloads: Vec::new(),
    }])
}

/// Reasons the kernel rejects a command.
///
/// Each variant describes a precondition violation or
/// constraint that prevents the transition from proceeding.
/// The runtime translates these into appropriate gRPC status
/// codes or internal retry decisions.
#[derive(Debug, Error, PartialEq)]
pub enum Reject {
    /// A workflow command carried invalid attributes. v1.31.0 FAILS THE
    /// WORKFLOW TASK with `cause` instead of rejecting the completion call
    /// (`failWorkflowTaskOnInvalidArgument`,
    /// respondworkflowtaskcompleted/workflow_task_completed_handler.go @
    /// v1.31.0); the completion applier converts this variant into a
    /// WFT-failed transition, so it never surfaces as a command reject.
    ///
    /// `message` mirrors v1.31.0's nullable `causeErr`: the API layer renders
    /// the wire message as `"{cause}: {message}"`, or the bare cause name when
    /// `None` (`workflowTaskFailedCause.Message()`,
    /// workflow_task_completed_handler.go:1502-1510 @ v1.31.0).
    #[error("invalid command attributes ({cause:?}): {}", message.as_deref().unwrap_or("<none>"))]
    InvalidCommandAttributes {
        cause: WorkflowTaskFailedCause,
        message: Option<String>,
    },
    /// A `Start` command was issued but the run already
    /// exists in durable storage.
    #[error("run already exists")]
    RunAlreadyExists,
    /// The run does not exist (expected `LoadedRun::Existing`).
    #[error("run not found")]
    MissingRun,
    /// History replay requires a non-empty sequence beginning with a start event.
    #[error("invalid replay history")]
    InvalidReplayHistory,
    /// The run has already reached a terminal status.
    #[error("run closed: {0:?}")]
    RunClosed(ExecutionStatus),
    /// A workflow task operation was attempted but no WFT is
    /// currently pending.
    #[error("no pending workflow task")]
    NoPendingWorkflowTask,
    /// The logical task sequence in the request does not match
    /// the pending WFT.
    #[error("workflow task sequence mismatch: expected {expected}, got {got}")]
    WorkflowTaskSeqMismatch { expected: u64, got: u64 },
    /// A `WorkflowTaskStarted` transition attempted to move a pinned workflow.
    #[error("pinned workflow cannot start a deployment-version transition")]
    PinnedWorkflowCannotTransition,
    /// A `WorkflowTaskStarted` was issued but the pending WFT
    /// has already been started.
    #[error("workflow task already started: logical_seq={logical_seq}")]
    WorkflowTaskAlreadyStarted { logical_seq: u64 },
    /// The completion token does not match the pending WFT's
    /// attempt or started event ID.
    #[error("workflow task token mismatch")]
    WorkflowTaskTokenMismatch,
    /// A completion or failure was issued but the pending WFT
    /// has not been started yet.
    #[error("workflow task not started: logical_seq={logical_seq}")]
    WorkflowTaskNotStarted { logical_seq: u64 },
    /// A `ScheduleActivity` command used an activity ID that
    /// is already in the open set.
    #[error("duplicate activity id: {0}")]
    DuplicateActivityId(String),
    /// A `StartTimer` command used a timer ID that is already
    /// in the open set.
    #[error("duplicate timer id: {0}")]
    DuplicateTimerId(String),
    /// An operation referenced an activity that does not exist
    /// in the open set.
    #[error("unknown activity: {0}")]
    UnknownActivity(String),
    /// An operation referenced a timer that does not exist in
    /// the open set.
    #[error("unknown timer: {0}")]
    UnknownTimer(String),
    /// A `StartChildWorkflow` command used a child workflow ID
    /// that is already in the open set.
    #[error("duplicate child workflow id: {0:?}")]
    DuplicateChildWorkflowId(tokeira_types::WorkflowId),
    /// An operation referenced a child workflow that does not
    /// exist in the open set.
    #[error("unknown child: {0:?}")]
    UnknownChild(tokeira_types::WorkflowId),
    /// A child start confirmation referenced a stale
    /// initiated event ID.
    #[error(
        "stale child confirmation for {child_workflow_id:?}: expected initiated_event_id {expected_initiated_event_id}"
    )]
    StaleChildConfirmation {
        child_workflow_id: tokeira_types::WorkflowId,
        expected_initiated_event_id: i64,
    },
    /// An external signal resolution referenced an unknown
    /// initiated event ID.
    #[error("unknown external signal: initiated_event_id={0}")]
    UnknownExternalSignal(i64),
    /// An external cancel resolution referenced an unknown
    /// initiated event ID.
    #[error("unknown external cancel: initiated_event_id={0}")]
    UnknownExternalCancel(i64),
    /// An update completion/rejection referenced an unknown
    /// update ID.
    #[error("unknown update: {0}")]
    UnknownUpdate(String),
    /// An update acceptance used an update ID that is already
    /// in the pending set.
    #[error("duplicate update id: {0}")]
    DuplicateUpdateId(String),
    /// A `ScheduleNexusOperation` command used an operation ID
    /// that is already in the pending set.
    #[error("duplicate nexus operation id: {0}")]
    DuplicateNexusOperationId(String),
    /// An operation referenced a Nexus operation that does not
    /// exist in the pending set.
    #[error("unknown nexus operation: {0}")]
    UnknownNexusOperation(String),
    /// A Nexus resolution referenced a stale scheduled event
    /// ID.
    #[error(
        "stale nexus resolution for {operation_id}: expected scheduled_event_id {expected_scheduled_event_id}"
    )]
    StaleNexusResolution {
        operation_id: String,
        expected_scheduled_event_id: i64,
    },
    /// A Nexus operation was already marked as started.
    #[error("nexus operation already started: {0}")]
    NexusOperationAlreadyStarted(String),
    /// A Nexus retry referenced an operation that is not backing off (already
    /// re-dispatched by a concurrent scanner tick, started, or resolved). The
    /// fence that prevents a double re-dispatch.
    #[error("nexus operation not backing off: {0}")]
    NexusOperationNotBackingOff(String),
    /// A `CompletionCallbackAttempted` referenced a callback index that is not in
    /// the run's `completion_callbacks`.
    #[error("unknown completion callback index: {0}")]
    UnknownCompletionCallback(usize),
    /// A `CompletionCallbackAttempted` targeted a callback already in a terminal
    /// state (`Succeeded`/`Failed`); a terminal callback is never re-attempted.
    #[error("completion callback {0} already terminal")]
    CompletionCallbackAlreadyTerminal(usize),
    /// The command requires the workflow to not be paused, but
    /// it is.
    #[error("workflow is paused")]
    WorkflowPaused,
    /// A pause was requested but the workflow is already
    /// paused.
    #[error("workflow is already paused")]
    AlreadyPaused,
    /// An unpause was requested but the workflow is not
    /// paused.
    #[error("workflow is not paused")]
    NotPaused,
    /// An activity unpause was requested but the activity is
    /// not paused.
    #[error("activity is not paused: {0}")]
    ActivityNotPaused(String),
    /// A reset command violated a structural constraint (e.g.
    /// invalid fork event ID).
    #[error("reset constraint violation: {reason}")]
    ResetConstraintViolation { reason: String },
    /// Workflow commands were issued after a close command
    /// within the same task completion.
    #[error("commands after close at index {index}")]
    CommandsAfterClose { index: usize },
    // TODO(correctness): add richer rejection reasons for
    // updates, continue-as-new constraints, child workflow
    // resolution mismatches, and cancellation races.
}
