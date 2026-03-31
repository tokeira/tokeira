use std::collections::BTreeMap;

use smallvec::SmallVec;
use thiserror::Error;
use time::OffsetDateTime;
use tokeira_types::{
    ExecutionStatus, LogicalTaskSeq, QueueKey, StickyAffinity, TransitionSeq,
};

use crate::{
    command::{
        ActivityResolvedRequest, Command, SignalRequest, StartRequest, StartWorkflowTaskRequest,
        TimerDueRequest, WorkflowCommand, WorkflowTaskCompletedRequest,
    },
    event::{ActivityResolution, HistoryEvent, HistoryEventKind},
    state::{
        ActivityState, LoadedRun, PendingWorkflowTask, TimerState, WorkflowState,
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
            Command::Signal(req) => self.apply_signal(loaded, req),
            Command::WorkflowTaskStarted(req) => self.apply_workflow_task_started(loaded, req),
            Command::WorkflowTaskCompleted(req) => self.apply_workflow_task_completed(loaded, req),
            Command::ActivityResolved(req) => self.apply_activity_resolved(loaded, req),
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
            activities: BTreeMap::new(),
            timers: BTreeMap::new(),
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
        } => {
            if builder.state.activities.contains_key(&activity_id) {
                return Err(Reject::DuplicateActivityId(activity_id));
            }

            let schedule_event_id = builder.emit(HistoryEventKind::ActivityTaskScheduled {
                activity_id: activity_id.clone(),
                task_queue: task_queue.clone(),
                input,
            });

            let activity = ActivityState {
                activity_id: activity_id.clone(),
                schedule_event_id,
                task_queue: task_queue.clone(),
                attempt: 1,
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
            Ok(true)
        }
        WorkflowCommand::FailWorkflow { message, details } => {
            builder.emit(HistoryEventKind::WorkflowExecutionFailed { message, details });
            builder.close(ExecutionStatus::Failed);
            Ok(true)
        }
        WorkflowCommand::RequestNewWorkflowTask => {
            if builder.state.pending_workflow_task.is_none() && builder.state.is_open() {
                builder.schedule_workflow_task();
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
        self.projection_ops.push(ProjectionOp::CloseExecution {
            status,
            closed_at: self.now,
        });
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
    #[error("commands after close at index {index}")]
    CommandsAfterClose { index: usize },

    // TODO(correctness): add richer rejection reasons for updates,
    // continue-as-new constraints, child workflow resolution mismatches, and
    // cancellation races.
}
