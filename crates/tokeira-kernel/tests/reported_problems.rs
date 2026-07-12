//! Consecutive workflow-task problem accounting (`TemporalReportedProblems`).
//!
//! The accumulator and problem identity live on `WorkflowState` and advance
//! inside the kernel's failed/timed-out transitions under exactly v1.31.0's
//! `failWorkflowTask` rules (`workflow_task_state_machine.go:1010-1027 @
//! v1.31.0`): non-sticky failures and non-sticky start-to-close timeouts
//! count (transient or not); a problem while the run's real sticky queue is
//! set counts nothing, clears the affinity, and retries fresh at attempt 1;
//! schedule-to-start timeouts never count; completion clears both fields.

use std::collections::BTreeMap;

use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    BasicKernel, Command, DispatchOp, LoadedRun, PendingWorkflowTask, StartWorkflowTaskRequest,
    VersioningBehavior, WorkflowState, WorkflowTaskCompletedRequest, WorkflowTaskFailedCause,
    WorkflowTaskFailedRequest, WorkflowTaskProblem, WorkflowTaskTimedOutRequest,
    WorkflowTaskTimeoutType,
    event::HistoryEventKind,
    kernel::Kernel,
};
use tokeira_types::{
    ExecutionStatus, LogicalTaskSeq, Memo, NamespaceId, RunId, RunKey, SearchAttributes,
    ShardEpoch, StickyAffinity, TaskQueueName, TransitionSeq, WorkerIdentity, WorkflowId,
    WorkflowTaskToken, WorkflowType,
};

fn now() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()
}

fn kernel() -> BasicKernel {
    BasicKernel
}

fn open_state() -> WorkflowState {
    WorkflowState {
        run_key: RunKey::new(),
        namespace_id: NamespaceId::new(),
        workflow_id: WorkflowId("workflow".into()),
        run_id: RunId::new(),
        workflow_type: WorkflowType("wf".into()),
        task_queue: TaskQueueName("queue".into()),
        deployment: None,
        build_id: None,
        versioning_info: None,
        worker_deployment_name: None,
        status: ExecutionStatus::Running,
        transition_seq: TransitionSeq(5),
        last_event_id: 9,
        next_workflow_task_seq: LogicalTaskSeq(4),
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
        admitted_updates: std::collections::HashSet::new(),
        pending_nexus_operations: BTreeMap::new(),
        completion_callbacks: Vec::new(),
        user_metadata: None,
        links: Vec::new(),
        workflow_start_delay: None,
        priority: None,
        started_at: now() - Duration::minutes(3),
        first_run_started_at: Some(now() - Duration::minutes(3)),
        closed_at: None,
        close_result: None,
        close_failure: None,
        request_id_infos: BTreeMap::new(),
        buffered_events: Vec::new(),
    }
}

/// Attempt-1 task with persisted Scheduled(8)/Started(9) events.
fn with_started_attempt1(mut state: WorkflowState) -> WorkflowState {
    state.pending_workflow_task = Some(PendingWorkflowTask {
        task_type: tokeira_kernel::WorkflowTaskType::Normal,
        schedule_to_start_deadline: None,
        logical_seq: LogicalTaskSeq(3),
        scheduled_event_id: 8,
        scheduled_at: now(),
        started_event_id: Some(9),
        started_at: Some(now()),
        attempt: 1,
    });
    state
}

/// Transient attempt-2 task with VIRTUAL ids (scheduled = last_event_id + 1).
fn with_started_attempt2_virtual(mut state: WorkflowState) -> WorkflowState {
    state.workflow_task_attempt = 2;
    state.pending_workflow_task = Some(PendingWorkflowTask {
        task_type: tokeira_kernel::WorkflowTaskType::Normal,
        schedule_to_start_deadline: None,
        logical_seq: LogicalTaskSeq(3),
        scheduled_event_id: 10,
        scheduled_at: now(),
        started_event_id: Some(11),
        started_at: Some(now()),
        attempt: 2,
    });
    state
}

fn real_sticky() -> StickyAffinity {
    StickyAffinity {
        worker_identity: WorkerIdentity("sticky-worker".into()),
        expires_at: now() + Duration::seconds(30),
        sticky_queue: TaskQueueName("queue:sticky".into()),
        schedule_to_start_timeout: Duration::seconds(5),
    }
}

fn fail_request(started_event_id: i64) -> WorkflowTaskFailedRequest {
    WorkflowTaskFailedRequest {
        logical_seq: LogicalTaskSeq(3),
        started_event_id,
        failure_cause: WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure,
        failure_details: None,
        worker_identity: WorkerIdentity("worker".into()),
        now: now(),
        reset_reapply: Vec::new(),
    }
}

#[test]
fn non_sticky_attempt1_failure_counts_and_records_cause() {
    let state = with_started_attempt1(open_state());
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::WorkflowTaskFailed(fail_request(9)),
        )
        .unwrap();

    assert_eq!(
        transition.next_state.workflow_task_attempts_since_last_success,
        1
    );
    assert_eq!(
        transition.next_state.last_workflow_task_problem,
        Some(WorkflowTaskProblem::Failed(
            WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure
        ))
    );
    // Transient retry retained at attempt 2 — no fresh Scheduled event.
    assert_eq!(transition.next_state.workflow_task_attempt, 2);
    assert!(matches!(
        transition.history_events[0].kind,
        HistoryEventKind::WorkflowTaskFailed { .. }
    ));
    assert_eq!(transition.history_events.len(), 1);
}

#[test]
fn transient_failure_counts_without_touching_recorded_cause() {
    let mut state = with_started_attempt2_virtual(open_state());
    state.workflow_task_attempts_since_last_success = 1;
    state.last_workflow_task_problem = Some(WorkflowTaskProblem::Failed(
        WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure,
    ));
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::WorkflowTaskFailed(WorkflowTaskFailedRequest {
                failure_cause: WorkflowTaskFailedCause::NonDeterminismError,
                ..fail_request(11)
            }),
        )
        .unwrap();

    // Counted, no event, and the recorded (attempt-1) cause is untouched —
    // v1.31.0 only rewrites `LastWorkflowTaskFailure` when the event is
    // emitted (workflow_task_state_machine.go:907-911 @ v1.31.0).
    assert_eq!(
        transition.next_state.workflow_task_attempts_since_last_success,
        2
    );
    assert_eq!(
        transition.next_state.last_workflow_task_problem,
        Some(WorkflowTaskProblem::Failed(
            WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure
        ))
    );
    assert!(transition.history_events.is_empty());
    assert_eq!(transition.next_state.workflow_task_attempt, 3);
}

#[test]
fn sticky_failure_counts_nothing_and_retries_fresh_attempt1() {
    let mut state = with_started_attempt1(open_state());
    state.sticky = Some(real_sticky());
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state.clone()),
            Command::WorkflowTaskFailed(fail_request(9)),
        )
        .unwrap();

    // No count; the recorded cause IS set (the event was emitted) — v1.31.0's
    // sticky rule suppresses only the increments
    // (workflow_task_state_machine.go:1010-1027 @ v1.31.0).
    assert_eq!(
        transition.next_state.workflow_task_attempts_since_last_success,
        0
    );
    assert_eq!(
        transition.next_state.last_workflow_task_problem,
        Some(WorkflowTaskProblem::Failed(
            WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure
        ))
    );
    // Sticky cleared; fresh attempt-1 retry with a REAL Scheduled event on
    // the normal queue ("clear sticky task queue first and try again before
    // creating transient workflow task").
    assert!(transition.next_state.sticky.is_none());
    assert_eq!(transition.next_state.workflow_task_attempt, 1);
    assert!(matches!(
        transition.history_events[0].kind,
        HistoryEventKind::WorkflowTaskFailed { .. }
    ));
    assert!(matches!(
        transition.history_events[1].kind,
        HistoryEventKind::WorkflowTaskScheduled { attempt: 1, .. }
    ));
    let enqueue = transition
        .dispatch_ops
        .iter()
        .find_map(|op| match op {
            DispatchOp::EnqueueWorkflowTask { queue, .. } => Some(queue),
            _ => None,
        })
        .expect("fresh retry enqueued");
    assert_eq!(enqueue.task_queue, state.task_queue);
}

#[test]
fn start_to_close_timeout_counts_like_a_failure() {
    let state = with_started_attempt1(open_state());
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest {
                logical_seq: LogicalTaskSeq(3),
                started_event_id: 9,
                timeout_type: WorkflowTaskTimeoutType::StartToClose,
                now: now(),
            }),
        )
        .unwrap();

    assert_eq!(
        transition.next_state.workflow_task_attempts_since_last_success,
        1
    );
    assert_eq!(
        transition.next_state.last_workflow_task_problem,
        Some(WorkflowTaskProblem::TimedOutStartToClose)
    );
    assert!(matches!(
        transition.history_events[0].kind,
        HistoryEventKind::WorkflowTaskTimedOut { .. }
    ));
}

#[test]
fn transient_timeout_counts_but_preserves_attempt1_failure_cause() {
    // Leaf-1 shape of Tier 3.22: explicit attempt-1 fail (counted, cause
    // recorded), then the SDK goes silent and the start-to-close timeout
    // drives the counter past the threshold while the SA keeps reporting the
    // attempt-1 cause.
    let mut state = with_started_attempt2_virtual(open_state());
    state.workflow_task_attempts_since_last_success = 1;
    state.last_workflow_task_problem = Some(WorkflowTaskProblem::Failed(
        WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure,
    ));
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest {
                logical_seq: LogicalTaskSeq(3),
                started_event_id: 11,
                timeout_type: WorkflowTaskTimeoutType::StartToClose,
                now: now(),
            }),
        )
        .unwrap();

    assert_eq!(
        transition.next_state.workflow_task_attempts_since_last_success,
        2
    );
    assert_eq!(
        transition.next_state.last_workflow_task_problem,
        Some(WorkflowTaskProblem::Failed(
            WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure
        ))
    );
    assert!(transition.history_events.is_empty());
}

#[test]
fn sticky_start_to_close_timeout_counts_nothing_and_resets_attempt() {
    let mut state = with_started_attempt1(open_state());
    state.sticky = Some(real_sticky());
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest {
                logical_seq: LogicalTaskSeq(3),
                started_event_id: 9,
                timeout_type: WorkflowTaskTimeoutType::StartToClose,
                now: now(),
            }),
        )
        .unwrap();

    assert_eq!(
        transition.next_state.workflow_task_attempts_since_last_success,
        0
    );
    assert!(transition.next_state.sticky.is_none());
    assert_eq!(transition.next_state.workflow_task_attempt, 1);
}

#[test]
fn schedule_to_start_timeout_never_counts() {
    let mut state = open_state();
    state.sticky = Some(real_sticky());
    state.pending_workflow_task = Some(PendingWorkflowTask {
        task_type: tokeira_kernel::WorkflowTaskType::Normal,
        schedule_to_start_deadline: Some(now()),
        logical_seq: LogicalTaskSeq(3),
        scheduled_event_id: 8,
        scheduled_at: now(),
        started_event_id: None,
        started_at: None,
        attempt: 1,
    });
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest {
                logical_seq: LogicalTaskSeq(3),
                started_event_id: 0,
                timeout_type: WorkflowTaskTimeoutType::ScheduleToStart,
                now: now(),
            }),
        )
        .unwrap();

    // v1.31.0: `failWorkflowTask(false)` — no count, no stored cause
    // (`ApplyWorkflowTaskTimedOutEvent`,
    // workflow_task_state_machine.go:263-268 @ v1.31.0).
    assert_eq!(
        transition.next_state.workflow_task_attempts_since_last_success,
        0
    );
    assert_eq!(transition.next_state.last_workflow_task_problem, None);
}

#[test]
fn completion_clears_count_and_recorded_problem() {
    let mut state = with_started_attempt1(open_state());
    state.workflow_task_attempts_since_last_success = 4;
    state.last_workflow_task_problem = Some(WorkflowTaskProblem::TimedOutStartToClose);
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state.clone()),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(3),
                    started_event_id: 9,
                    attempt: 1,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
                now: now(),
            }),
        )
        .unwrap();

    // v1.31.0 zeroes the accumulator and removes the SA plus its stored
    // cause on success (workflow_task_state_machine.go:838-846,
    // mutable_state_impl.go:6530-6541 @ v1.31.0).
    assert_eq!(
        transition.next_state.workflow_task_attempts_since_last_success,
        0
    );
    assert_eq!(transition.next_state.last_workflow_task_problem, None);
}

#[test]
fn poll_hint_does_not_clobber_real_sticky_affinity() {
    let mut state = open_state();
    state.sticky = Some(real_sticky());
    state.pending_workflow_task = Some(PendingWorkflowTask {
        task_type: tokeira_kernel::WorkflowTaskType::Normal,
        schedule_to_start_deadline: None,
        logical_seq: LogicalTaskSeq(3),
        scheduled_event_id: 8,
        scheduled_at: now(),
        started_event_id: None,
        started_at: None,
        attempt: 1,
    });
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::WorkflowTaskStarted(StartWorkflowTaskRequest {
                logical_seq: LogicalTaskSeq(3),
                worker_identity: WorkerIdentity("worker".into()),
                request_id: "req-1".into(),
                history_size_bytes: 0,
                suggest_continue_as_new: false,
                deployment_transition: None,
                deployment_transition_revision_number: None,
                sticky_ttl: Some(Duration::seconds(30)),
                now: now(),
            }),
        )
        .unwrap();

    // The poll-side hint (empty queue name) must not erase the real (S1)
    // sticky queue: v1.31.0's RecordWorkflowTaskStarted never touches it, and
    // the failure path's sticky rule reads it at fail time (raise S4).
    let sticky = transition.next_state.sticky.expect("sticky retained");
    assert_eq!(sticky.sticky_queue, TaskQueueName("queue:sticky".into()));
}
