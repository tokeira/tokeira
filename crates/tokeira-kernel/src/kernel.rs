use std::collections::BTreeMap;

use smallvec::SmallVec;
use thiserror::Error;
use time::OffsetDateTime;
use tokeira_types::{
    ExecutionStatus, LogicalTaskSeq, QueueKey, StickyAffinity, TransitionSeq,
};

use crate::{
    command::{
        ActivityResolvedRequest, CancelRequest, ChildResolution, ChildResolvedRequest,
        ChildStartConfirmedRequest, ChildStartResult, Command, ExternalCancelResolvedRequest,
        ExternalCancelResult, ExternalSignalResolvedRequest, ExternalSignalResult, RetryState,
        SignalRequest, StartRequest, StartWorkflowTaskRequest, TerminateRequest, TimerDueRequest,
        UpdateProtocolBody, UpdateRequest, WorkflowCommand, WorkflowExecutionTimedOutRequest,
        WorkflowTaskCompletedRequest, WorkflowTaskFailedRequest, WorkflowTaskTimedOutRequest,
    },
    event::{ActivityResolution, HistoryEvent, HistoryEventKind},
    state::{
        ActivityState, ChildWorkflowState, LoadedRun, ParentClosePolicy, PendingExternalCancel,
        PendingExternalSignal, PendingUpdate, PendingWorkflowTask, TimerState, WorkflowState,
    },
    transition::{
        ActivityOp, DispatchOp, ProjectionOp, RequestDedupeOp, TimerOp, Transition,
    },
};

/// Pure transition engine.
pub trait Kernel {
    fn apply(&self, loaded: LoadedRun, command: Command) -> Result<Transition, Reject>;
}

#[derive(Default)]
pub struct BasicKernel;

impl Kernel for BasicKernel {
    fn apply(&self, loaded: LoadedRun, command: Command) -> Result<Transition, Reject> {
        match command {
            Command::Start(req) => self.apply_start(loaded, req),
            Command::Update(req) => self.apply_update(loaded, req),
            Command::Signal(req) => self.apply_signal(loaded, req),
            Command::Cancel(req) => self.apply_cancel(loaded, req),
            Command::Terminate(req) => self.apply_terminate(loaded, req),
            Command::WorkflowExecutionTimedOut(req) => {
                self.apply_workflow_execution_timed_out(loaded, req)
            }
            Command::WorkflowTaskStarted(req) => self.apply_workflow_task_started(loaded, req),
            Command::WorkflowTaskCompleted(req) => self.apply_workflow_task_completed(loaded, req),
            Command::WorkflowTaskFailed(req) => self.apply_workflow_task_failed(loaded, req),
            Command::WorkflowTaskTimedOut(req) => self.apply_workflow_task_timed_out(loaded, req),
            Command::ActivityResolved(req) => self.apply_activity_resolved(loaded, req),
            Command::ChildStartConfirmed(req) => self.apply_child_start_confirmed(loaded, req),
            Command::ChildResolved(req) => self.apply_child_resolved(loaded, req),
            Command::ExternalSignalResolved(req) => self.apply_external_signal_resolved(loaded, req),
            Command::ExternalCancelResolved(req) => self.apply_external_cancel_resolved(loaded, req),
            Command::TimerDue(req) => self.apply_timer_due(loaded, req),
        }
    }
}

impl BasicKernel {
    fn apply_start(&self, loaded: LoadedRun, req: StartRequest) -> Result<Transition, Reject> {
        if !matches!(loaded, LoadedRun::Absent) {
            return Err(Reject::RunAlreadyExists);
        }

        let initial = WorkflowState {
            run_key: req.run_key,
            namespace_id: req.namespace_id,
            workflow_id: req.workflow_id,
            run_id: req.run_id,
            workflow_type: req.workflow_type.clone(),
            task_queue: req.task_queue.clone(),
            status: ExecutionStatus::Running,
            transition_seq: TransitionSeq::ZERO,
            last_event_id: 0,
            next_workflow_task_seq: LogicalTaskSeq::ONE,
            pending_workflow_task: None,
            sticky: None,
            memo: req.memo.clone(),
            search_attributes: req.search_attributes.clone(),
            workflow_execution_timeout: req.workflow_execution_timeout,
            workflow_run_timeout: req.workflow_run_timeout,
            workflow_task_timeout: req.workflow_task_timeout,
            retry_policy: req.retry_policy.clone(),
            attempt: req.attempt,
            activities: BTreeMap::new(),
            timers: BTreeMap::new(),
            children: BTreeMap::new(),
            pending_external_signals: BTreeMap::new(),
            pending_external_cancels: BTreeMap::new(),
            pending_updates: BTreeMap::new(),
            started_at: req.now,
            closed_at: None,
        };

        let mut builder = TransitionBuilder::new(initial, req.now);
        builder.request_dedupe_ops.push(RequestDedupeOp {
            request_id: req.request.request_id.clone(),
        });
        builder.emit(HistoryEventKind::WorkflowExecutionStarted {
            workflow_type: req.workflow_type,
            task_queue: req.task_queue,
            input: req.input,
            memo: req.memo.clone(),
            search_attributes: req.search_attributes.clone(),
            request_id: req.request.request_id.0,
            continued_execution_run_id: req.continued_execution_run_id,
            first_execution_run_id: req.first_execution_run_id,
            retry_policy: req.retry_policy,
            attempt: req.attempt,
            workflow_execution_timeout: req.workflow_execution_timeout,
            workflow_run_timeout: req.workflow_run_timeout,
            workflow_task_timeout: req.workflow_task_timeout,
        });
        builder.projection_ops.push(ProjectionOp::UpsertExecution {
            status: ExecutionStatus::Running,
            memo_patch: req.memo,
            search_attr_patch: req.search_attributes,
        });
        builder.schedule_workflow_task();
        Ok(builder.finish())
    }

    fn apply_signal(&self, loaded: LoadedRun, req: SignalRequest) -> Result<Transition, Reject> {
        let state = expect_open(loaded)?;
        let mut builder = TransitionBuilder::new(state, req.now);
        builder.request_dedupe_ops.push(RequestDedupeOp {
            request_id: req.request.request_id.clone(),
        });

        builder.emit(HistoryEventKind::WorkflowExecutionSignaled {
            signal_name: req.signal_name,
            input: req.input,
            request_id: req.request.request_id.0,
            identity: req.request.caller_identity,
        });

        // Insight: Tokeira keeps the "at most one outstanding workflow task"
        // invariant because it dramatically reduces wakeup amplification during
        // signal floods without weakening per-run correctness.
        if builder.state.pending_workflow_task.is_none() {
            builder.schedule_workflow_task();
        }

        Ok(builder.finish())
    }

    fn apply_update(&self, loaded: LoadedRun, req: UpdateRequest) -> Result<Transition, Reject> {
        let state = expect_open(loaded)?;
        if state.pending_updates.contains_key(&req.update_id) {
            return Err(Reject::DuplicateUpdateId(req.update_id));
        }

        let mut builder = TransitionBuilder::new(state, req.now);
        builder.request_dedupe_ops.push(RequestDedupeOp {
            request_id: req.request.request_id.clone(),
        });
        let accepted_event_id = builder.emit(HistoryEventKind::WorkflowExecutionUpdateAccepted {
            update_id: req.update_id.clone(),
            update_name: req.update_name.clone(),
            input: req.input,
        });
        builder.state.pending_updates.insert(
            req.update_id.clone(),
            PendingUpdate {
                update_id: req.update_id,
                accepted_event_id,
                name: req.update_name,
            },
        );

        if builder.state.pending_workflow_task.is_none() {
            builder.schedule_workflow_task();
        }

        Ok(builder.finish())
    }

    fn apply_cancel(&self, loaded: LoadedRun, req: CancelRequest) -> Result<Transition, Reject> {
        let state = expect_open(loaded)?;
        let mut builder = TransitionBuilder::new(state, req.now);
        builder.request_dedupe_ops.push(RequestDedupeOp {
            request_id: req.request.request_id.clone(),
        });
        builder.emit(HistoryEventKind::WorkflowExecutionCancelRequested {
            reason: req.reason,
            external_workflow_execution: req.external_initiator,
            request_id: req.request.request_id.0,
        });

        if builder.state.pending_workflow_task.is_none() {
            builder.schedule_workflow_task();
        }

        Ok(builder.finish())
    }

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
        builder.emit(HistoryEventKind::WorkflowExecutionTerminated {
            reason: req.reason,
            details: req.details,
            identity: req.identity,
        });
        builder.close(ExecutionStatus::Terminated);

        let activities = std::mem::take(&mut builder.state.activities);
        for (activity_id, _) in activities {
            builder.activity_ops.push(ActivityOp::Delete { activity_id });
        }

        let timers = std::mem::take(&mut builder.state.timers);
        for (timer_id, _) in timers {
            builder.timer_ops.push(TimerOp::Delete { timer_id });
        }

        builder.apply_parent_close_policy();

        Ok(builder.finish())
    }

    fn apply_workflow_execution_timed_out(
        &self,
        loaded: LoadedRun,
        req: WorkflowExecutionTimedOutRequest,
    ) -> Result<Transition, Reject> {
        let state = expect_open(loaded)?;
        let mut builder = TransitionBuilder::new(state, req.now);
        builder.emit(HistoryEventKind::WorkflowExecutionTimedOut {
            timeout_type: req.timeout_type,
            retry_state: req.retry_state,
        });
        builder.close(ExecutionStatus::TimedOut);

        let activities = std::mem::take(&mut builder.state.activities);
        for (activity_id, _) in activities {
            builder.activity_ops.push(ActivityOp::Delete { activity_id });
        }

        let timers = std::mem::take(&mut builder.state.timers);
        for (timer_id, _) in timers {
            builder.timer_ops.push(TimerOp::Delete { timer_id });
        }

        builder.apply_parent_close_policy();

        Ok(builder.finish())
    }

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
        let attempt = pending.attempt + 1;
        let started_event_id = builder.emit(HistoryEventKind::WorkflowTaskStarted {
            logical_seq: pending.logical_seq,
            scheduled_event_id: pending.scheduled_event_id,
            attempt,
            identity: req.worker_identity.clone(),
        });

        let current = builder
            .state
            .pending_workflow_task
            .as_mut()
            .expect("validated pending workflow task must still exist");
        current.started_event_id = Some(started_event_id);
        current.attempt = attempt;

        if let Some(ttl) = req.sticky_ttl {
            builder.state.sticky = Some(StickyAffinity {
                worker_identity: req.worker_identity,
                expires_at: req.now + ttl,
            });
        }

        Ok(builder.finish())
    }

    fn apply_workflow_task_completed(
        &self,
        loaded: LoadedRun,
        req: WorkflowTaskCompletedRequest,
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
        builder.emit(HistoryEventKind::WorkflowTaskCompleted {
            logical_seq: req.token.logical_seq,
            scheduled_event_id: pending.scheduled_event_id,
            started_event_id: req.token.started_event_id,
            identity: req.identity,
        });
        builder.state.pending_workflow_task = None;

        let mut closed = false;
        for (index, command) in req.commands.into_iter().enumerate() {
            if closed {
                return Err(Reject::CommandsAfterClose { index });
            }
            closed = apply_workflow_command(&mut builder, command)?;
        }

        if req.force_new_workflow_task && builder.state.is_open() && builder.state.pending_workflow_task.is_none() {
            builder.schedule_workflow_task();
        }

        Ok(builder.finish())
    }

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
                    result,
                });
            }
            ActivityResolution::Failed { message } => {
                builder.emit(HistoryEventKind::ActivityTaskFailed {
                    activity_id: activity.activity_id.clone(),
                    message,
                });
            }
            ActivityResolution::TimedOut { timeout_type } => {
                builder.emit(HistoryEventKind::ActivityTaskTimedOut {
                    activity_id: activity.activity_id.clone(),
                    timeout_type,
                });
            }
            ActivityResolution::Canceled { details } => {
                builder.emit(HistoryEventKind::ActivityTaskCanceled {
                    activity_id: activity.activity_id.clone(),
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
                workflow_type,
            } => {
                let child_run_id_for_state = child_run_id;
                let started_event_id = builder.emit(HistoryEventKind::ChildWorkflowExecutionStarted {
                    child_workflow_id: child.child_workflow_id.clone(),
                    child_run_id,
                    workflow_type,
                });
                if let Some(current) = builder.state.children.get_mut(&child.child_workflow_id) {
                    current.child_run_id = Some(child_run_id_for_state);
                    current.started_event_id = Some(started_event_id);
                }
            }
            ChildStartResult::Failed { cause } => {
                builder.emit(HistoryEventKind::StartChildWorkflowExecutionFailed {
                    child_workflow_id: child.child_workflow_id.clone(),
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
                    result,
                });
            }
            ChildResolution::Failed { failure } => {
                builder.emit(HistoryEventKind::ChildWorkflowExecutionFailed {
                    child_workflow_id: child.child_workflow_id.clone(),
                    failure,
                });
            }
            ChildResolution::Canceled => {
                builder.emit(HistoryEventKind::ChildWorkflowExecutionCanceled {
                    child_workflow_id: child.child_workflow_id.clone(),
                });
            }
            ChildResolution::Terminated => {
                builder.emit(HistoryEventKind::ChildWorkflowExecutionTerminated {
                    child_workflow_id: child.child_workflow_id.clone(),
                });
            }
            ChildResolution::TimedOut => {
                builder.emit(HistoryEventKind::ChildWorkflowExecutionTimedOut {
                    child_workflow_id: child.child_workflow_id.clone(),
                });
            }
        }

        builder.state.children.remove(&child.child_workflow_id);

        if builder.state.pending_workflow_task.is_none() {
            builder.schedule_workflow_task();
        }

        Ok(builder.finish())
    }

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
                    target_workflow_id: pending.target_workflow_id,
                });
            }
            ExternalSignalResult::Failed { cause } => {
                builder.emit(HistoryEventKind::SignalExternalWorkflowExecutionFailed {
                    initiated_event_id: pending.initiated_event_id,
                    target_workflow_id: pending.target_workflow_id,
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
                    target_workflow_id: pending.target_workflow_id,
                });
            }
            ExternalCancelResult::Failed { cause } => {
                builder.emit(HistoryEventKind::RequestCancelExternalWorkflowExecutionFailed {
                    initiated_event_id: pending.initiated_event_id,
                    target_workflow_id: pending.target_workflow_id,
                    cause,
                });
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
        builder.emit(HistoryEventKind::WorkflowTaskFailed {
            logical_seq: pending.logical_seq,
            scheduled_event_id: pending.scheduled_event_id,
            started_event_id,
            failure_cause: req.failure_cause,
            failure_details: req.failure_details,
            identity: req.worker_identity,
        });
        let sticky_preferred = builder
            .state
            .sticky
            .as_ref()
            .map(|sticky| sticky.worker_identity.clone());
        let current = builder
            .state
            .pending_workflow_task
            .as_mut()
            .expect("validated pending workflow task must still exist");
        current.started_event_id = None;
        builder.dispatch_ops.push(DispatchOp::EnqueueWorkflowTask {
            queue: QueueKey {
                namespace_id: builder.state.namespace_id,
                task_queue: builder.state.task_queue.clone(),
                task_kind: tokeira_types::TaskKind::Workflow,
                deployment: None,
                build_id: None,
            },
            logical_seq: pending.logical_seq,
            sticky_preferred,
        });
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
        builder.emit(HistoryEventKind::WorkflowTaskTimedOut {
            logical_seq: pending.logical_seq,
            scheduled_event_id: pending.scheduled_event_id,
            started_event_id,
            timeout_type: req.timeout_type,
        });
        let current = builder
            .state
            .pending_workflow_task
            .as_mut()
            .expect("validated pending workflow task must still exist");
        current.started_event_id = None;
        builder.state.sticky = None;
        builder.dispatch_ops.push(DispatchOp::EnqueueWorkflowTask {
            queue: QueueKey {
                namespace_id: builder.state.namespace_id,
                task_queue: builder.state.task_queue.clone(),
                task_kind: tokeira_types::TaskKind::Workflow,
                deployment: None,
                build_id: None,
            },
            logical_seq: pending.logical_seq,
            sticky_preferred: None,
        });
        Ok(builder.finish())
    }

    fn apply_timer_due(&self, loaded: LoadedRun, req: TimerDueRequest) -> Result<Transition, Reject> {
        let state = expect_open(loaded)?;
        let timer = state
            .timers
            .get(&req.timer_id)
            .cloned()
            .ok_or_else(|| Reject::UnknownTimer(req.timer_id.clone()))?;

        let mut builder = TransitionBuilder::new(state, req.fired_at);
        builder.emit(HistoryEventKind::TimerFired {
            timer_id: timer.timer_id.clone(),
        });
        builder.state.timers.remove(&timer.timer_id);
        builder.timer_ops.push(TimerOp::Delete {
            timer_id: timer.timer_id,
        });

        if builder.state.pending_workflow_task.is_none() {
            builder.schedule_workflow_task();
        }

        Ok(builder.finish())
    }
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

fn apply_workflow_command(builder: &mut TransitionBuilder, command: WorkflowCommand) -> Result<bool, Reject> {
    match command {
        WorkflowCommand::ScheduleActivity {
            activity_id,
            task_queue,
            input,
            schedule_to_close_timeout,
            schedule_to_start_timeout,
            start_to_close_timeout,
            heartbeat_timeout,
        } => {
            if builder.state.activities.contains_key(&activity_id) {
                return Err(Reject::DuplicateActivityId(activity_id));
            }

            let schedule_event_id = builder.emit(HistoryEventKind::ActivityTaskScheduled {
                activity_id: activity_id.clone(),
                task_queue: task_queue.clone(),
                input,
                schedule_to_close_timeout,
                schedule_to_start_timeout,
                start_to_close_timeout,
                heartbeat_timeout,
            });

            let activity = ActivityState {
                activity_id: activity_id.clone(),
                schedule_event_id,
                task_queue: task_queue.clone(),
                attempt: 1,
                schedule_to_close_timeout,
                schedule_to_start_timeout,
                start_to_close_timeout,
                heartbeat_timeout,
            };
            builder.state.activities.insert(activity_id.clone(), activity.clone());
            builder.activity_ops.push(ActivityOp::Upsert(activity));
            builder.dispatch_ops.push(DispatchOp::EnqueueActivityTask {
                queue: QueueKey {
                    namespace_id: builder.state.namespace_id,
                    task_queue,
                    task_kind: tokeira_types::TaskKind::Activity,
                    deployment: None,
                    build_id: None,
                },
                activity_id,
                schedule_event_id,
                attempt: 1,
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
        WorkflowCommand::CompleteWorkflow { result } => {
            builder.emit(HistoryEventKind::WorkflowExecutionCompleted { result });
            builder.close(ExecutionStatus::Completed);
            builder.apply_parent_close_policy();
            Ok(true)
        }
        WorkflowCommand::FailWorkflow { message, details } => {
            let retry_state = if builder.state.retry_policy.is_some() {
                RetryState::InProgress
            } else {
                RetryState::RetryPolicyNotSet
            };
            let attempt = builder.state.attempt;
            builder.emit(HistoryEventKind::WorkflowExecutionFailed {
                message,
                details,
                retry_state,
                attempt,
            });
            builder.close(ExecutionStatus::Failed);
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
        } => {
            builder.emit(HistoryEventKind::WorkflowExecutionContinuedAsNew {
                new_run_id,
                workflow_type,
                task_queue,
                input,
                memo,
                search_attributes,
                workflow_execution_timeout,
                workflow_run_timeout,
                workflow_task_timeout,
            });
            builder.close(ExecutionStatus::ContinuedAsNew);
            builder.apply_parent_close_policy();
            Ok(true)
        }
        WorkflowCommand::CancelWorkflow => {
            builder.emit(HistoryEventKind::WorkflowExecutionCanceled);
            builder.close(ExecutionStatus::Cancelled);
            builder.apply_parent_close_policy();
            Ok(true)
        }
        WorkflowCommand::RequestCancelActivity { activity_id } => {
            if !builder.state.activities.contains_key(&activity_id) {
                return Err(Reject::UnknownActivity(activity_id));
            }
            builder.emit(HistoryEventKind::ActivityTaskCancelRequested { activity_id });
            Ok(false)
        }
        WorkflowCommand::CancelTimer { timer_id } => {
            if !builder.state.timers.contains_key(&timer_id) {
                return Err(Reject::UnknownTimer(timer_id));
            }
            builder.emit(HistoryEventKind::TimerCanceled {
                timer_id: timer_id.clone(),
            });
            builder.state.timers.remove(&timer_id);
            builder.timer_ops.push(TimerOp::Delete { timer_id });
            Ok(false)
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
            workflow_type,
            task_queue,
            input,
            parent_close_policy,
        } => {
            if builder.state.children.contains_key(&child_workflow_id) {
                return Err(Reject::DuplicateChildWorkflowId(child_workflow_id));
            }
            let initiated_event_id = builder.emit(HistoryEventKind::StartChildWorkflowExecutionInitiated {
                child_workflow_id: child_workflow_id.clone(),
                workflow_type: workflow_type.clone(),
                task_queue: task_queue.clone(),
                input: input.clone(),
                namespace_id,
                parent_close_policy,
            });
            builder.state.children.insert(
                child_workflow_id.clone(),
                ChildWorkflowState {
                    child_workflow_id: child_workflow_id.clone(),
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
            });
            Ok(false)
        }
        WorkflowCommand::SignalExternalWorkflowExecution {
            target_workflow_id,
            target_run_id,
            signal_name,
            input,
        } => {
            let initiated_event_id = builder.emit(
                HistoryEventKind::SignalExternalWorkflowExecutionInitiated {
                    target_workflow_id: target_workflow_id.clone(),
                    target_run_id,
                    signal_name: signal_name.clone(),
                    input: input.clone(),
                },
            );
            builder.state.pending_external_signals.insert(
                initiated_event_id,
                PendingExternalSignal {
                    initiated_event_id,
                    target_workflow_id: target_workflow_id.clone(),
                    target_run_id,
                    signal_name: signal_name.clone(),
                },
            );
            builder.dispatch_ops.push(DispatchOp::SignalExternalWorkflow {
                target_workflow_id,
                target_run_id,
                signal_name,
                input,
            });
            Ok(false)
        }
        WorkflowCommand::RequestCancelExternalWorkflowExecution {
            target_workflow_id,
            target_run_id,
        } => {
            let initiated_event_id = builder.emit(
                HistoryEventKind::RequestCancelExternalWorkflowExecutionInitiated {
                    target_workflow_id: target_workflow_id.clone(),
                    target_run_id,
                },
            );
            builder.state.pending_external_cancels.insert(
                initiated_event_id,
                PendingExternalCancel {
                    initiated_event_id,
                    target_workflow_id: target_workflow_id.clone(),
                    target_run_id,
                },
            );
            builder
                .dispatch_ops
                .push(DispatchOp::RequestCancelExternalWorkflow {
                    target_workflow_id,
                    target_run_id,
                });
            Ok(false)
        }
        WorkflowCommand::UpdateCompleted { update_id, result } => {
            if !builder.state.pending_updates.contains_key(&update_id) {
                return Err(Reject::UnknownUpdate(update_id));
            }
            builder.emit(HistoryEventKind::WorkflowExecutionUpdateCompleted {
                update_id: update_id.clone(),
                result,
            });
            builder.state.pending_updates.remove(&update_id);
            Ok(false)
        }
        WorkflowCommand::UpdateRejected { update_id, failure } => {
            if !builder.state.pending_updates.contains_key(&update_id) {
                return Err(Reject::UnknownUpdate(update_id));
            }
            builder.emit(HistoryEventKind::WorkflowExecutionUpdateRejected {
                update_id: update_id.clone(),
                failure,
            });
            builder.state.pending_updates.remove(&update_id);
            Ok(false)
        }
        WorkflowCommand::ProtocolMessage { message_id: _, body } => {
            match body {
                UpdateProtocolBody::Accepted {
                    update_id,
                    update_name,
                    input,
                } => {
                    if builder.state.pending_updates.contains_key(&update_id) {
                        return Err(Reject::DuplicateUpdateId(update_id));
                    }
                    let accepted_event_id =
                        builder.emit(HistoryEventKind::WorkflowExecutionUpdateAccepted {
                            update_id: update_id.clone(),
                            update_name: update_name.clone(),
                            input,
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
                    });
                    builder.state.pending_updates.remove(&update_id);
                }
                UpdateProtocolBody::Rejected { update_id, failure } => {
                    if !builder.state.pending_updates.contains_key(&update_id) {
                        return Err(Reject::UnknownUpdate(update_id));
                    }
                    builder.emit(HistoryEventKind::WorkflowExecutionUpdateRejected {
                        update_id: update_id.clone(),
                        failure,
                    });
                    builder.state.pending_updates.remove(&update_id);
                }
            }
            Ok(false)
        }
    }
}

struct TransitionBuilder {
    state: WorkflowState,
    now: OffsetDateTime,
    history_events: SmallVec<[HistoryEvent; 8]>,
    request_dedupe_ops: SmallVec<[RequestDedupeOp; 1]>,
    activity_ops: SmallVec<[ActivityOp; 4]>,
    timer_ops: SmallVec<[TimerOp; 4]>,
    dispatch_ops: SmallVec<[DispatchOp; 4]>,
    projection_ops: SmallVec<[ProjectionOp; 8]>,
    expected_seq: TransitionSeq,
}

impl TransitionBuilder {
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

    fn emit(&mut self, kind: HistoryEventKind) -> i64 {
        let event_id = self.state.last_event_id + 1;
        self.state.last_event_id = event_id;
        self.history_events.push(HistoryEvent {
            event_id,
            happened_at: self.now,
            kind,
        });
        event_id
    }

    fn schedule_workflow_task(&mut self) {
        let logical_seq = self.state.next_workflow_task_seq;
        self.state.next_workflow_task_seq = logical_seq.next();
        let scheduled_event_id = self.emit(HistoryEventKind::WorkflowTaskScheduled { logical_seq });
        self.state.pending_workflow_task = Some(PendingWorkflowTask {
            logical_seq,
            scheduled_event_id,
            started_event_id: None,
            attempt: 0,
        });
        self.dispatch_ops.push(DispatchOp::EnqueueWorkflowTask {
            queue: QueueKey {
                namespace_id: self.state.namespace_id,
                task_queue: self.state.task_queue.clone(),
                task_kind: tokeira_types::TaskKind::Workflow,
                deployment: None,
                build_id: None,
            },
            logical_seq,
            sticky_preferred: self.state.sticky.as_ref().map(|s| s.worker_identity.clone()),
        });
    }

    fn close(&mut self, status: ExecutionStatus) {
        self.state.status = status;
        self.state.closed_at = Some(self.now);
        self.state.pending_workflow_task = None;
        self.state.sticky = None;
        self.state.pending_external_signals.clear();
        self.state.pending_external_cancels.clear();
        self.state.pending_updates.clear();
        self.projection_ops.push(ProjectionOp::CloseExecution {
            status,
            closed_at: self.now,
        });
    }

    fn apply_parent_close_policy(&mut self) {
        let children = std::mem::take(&mut self.state.children);
        for (_, child) in children {
            let Some(child_run_id) = child.child_run_id else {
                continue;
            };
            match child.parent_close_policy {
                ParentClosePolicy::Terminate => {
                    self.dispatch_ops.push(DispatchOp::TerminateChild {
                        child_workflow_id: child.child_workflow_id,
                        child_run_id,
                        reason: "parent workflow closed".into(),
                    });
                }
                ParentClosePolicy::RequestCancel => {
                    self.dispatch_ops.push(DispatchOp::CancelChild {
                        child_workflow_id: child.child_workflow_id,
                        child_run_id,
                        reason: "parent workflow closed".into(),
                    });
                }
                ParentClosePolicy::Abandon => {}
            }
        }
    }

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

#[derive(Debug, Error, PartialEq)]
pub enum Reject {
    #[error("run already exists")]
    RunAlreadyExists,
    #[error("run not found")]
    MissingRun,
    #[error("run closed: {0:?}")]
    RunClosed(ExecutionStatus),
    #[error("no pending workflow task")]
    NoPendingWorkflowTask,
    #[error("workflow task sequence mismatch: expected {expected}, got {got}")]
    WorkflowTaskSeqMismatch { expected: u64, got: u64 },
    #[error("workflow task already started: logical_seq={logical_seq}")]
    WorkflowTaskAlreadyStarted { logical_seq: u64 },
    #[error("workflow task token mismatch")]
    WorkflowTaskTokenMismatch,
    #[error("workflow task not started: logical_seq={logical_seq}")]
    WorkflowTaskNotStarted { logical_seq: u64 },
    #[error("duplicate activity id: {0}")]
    DuplicateActivityId(String),
    #[error("duplicate timer id: {0}")]
    DuplicateTimerId(String),
    #[error("unknown activity: {0}")]
    UnknownActivity(String),
    #[error("unknown timer: {0}")]
    UnknownTimer(String),
    #[error("duplicate child workflow id: {0:?}")]
    DuplicateChildWorkflowId(tokeira_types::WorkflowId),
    #[error("unknown child: {0:?}")]
    UnknownChild(tokeira_types::WorkflowId),
    #[error("stale child confirmation for {child_workflow_id:?}: expected initiated_event_id {expected_initiated_event_id}")]
    StaleChildConfirmation {
        child_workflow_id: tokeira_types::WorkflowId,
        expected_initiated_event_id: i64,
    },
    #[error("unknown external signal: initiated_event_id={0}")]
    UnknownExternalSignal(i64),
    #[error("unknown external cancel: initiated_event_id={0}")]
    UnknownExternalCancel(i64),
    #[error("unknown update: {0}")]
    UnknownUpdate(String),
    #[error("duplicate update id: {0}")]
    DuplicateUpdateId(String),
    #[error("commands after close at index {index}")]
    CommandsAfterClose { index: usize },

    // TODO(correctness): add richer rejection reasons for updates,
    // continue-as-new constraints, child workflow resolution mismatches, and
    // cancellation races.
}
