//! `run-growth-limits`: the kernel marks the events Temporal v1.31.0 numbers
//! only when it finishes a write, which the history count check leaves out,
//! and force-closes a started workflow task when its buffered events pass the
//! count or size limit, whichever kind of event buffered.
// Integration test: unwrap is idiomatic in test code (root AGENTS.md §1).
#![allow(clippy::unwrap_used)]

use std::{
    collections::{BTreeMap, HashSet},
    sync::OnceLock,
};

use proptest::prelude::*;
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    ActivityResolution, ActivityResolvedRequest, ActivityStartMode, ActivityStartRequest,
    BasicKernel, Command, ContinueAsNewAdvicePolicy, LoadedRun, NexusOperationResolvedRequest,
    NexusResolution, PendingNexusOperation, PendingWorkflowTask, SignalRequest,
    StartWorkflowTaskRequest, Transition, WorkflowCommand, WorkflowState,
    WorkflowTaskCompletedRequest, WorkflowTaskCompletionLimits, WorkflowTaskFailedCause,
    WorkflowTaskType,
    event::HistoryEventKind,
    kernel::Kernel,
    limits::{MAXIMUM_BUFFERED_EVENTS_BATCH, MAXIMUM_BUFFERED_EVENTS_SIZE_IN_BYTES},
    payload_size::payloads_encoded_len,
};
use tokeira_types::{
    ExecutionStatus, LogicalTaskSeq, Memo, NamespaceId, Payload, Payloads, RequestContext,
    RequestId, RunId, RunKey, SearchAttributes, ShardEpoch, TaskQueueName, TransitionSeq,
    WorkerIdentity, WorkflowId, WorkflowTaskToken, WorkflowType,
};

fn now() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()
}

/// One payload whose data is `size` bytes.
fn payloads_of(size: usize) -> Payloads {
    Payloads(vec![Payload::new(vec![7; size])])
}

fn request(id: &str) -> RequestContext {
    RequestContext {
        request_id: RequestId(id.into()),
        caller_identity: Some("tester".into()),
        principal: None,
        received_at: now(),
    }
}

/// An open run with 14 events and no pending workflow task.
fn open_state() -> WorkflowState {
    let now = now();
    WorkflowState {
        completed_update_count: 0,
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
        transition_seq: TransitionSeq(7),
        last_event_id: 14,
        external_payload_count: 0,
        external_payload_size_bytes: 0,
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
        workflow_execution_timeout: Some(Duration::minutes(5)),
        workflow_run_timeout: Some(Duration::minutes(1)),
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
        started_at: now - Duration::minutes(10),
        first_run_started_at: Some(now - Duration::minutes(10)),
        closed_at: None,
        close_result: None,
        close_failure: None,
        request_id_infos: BTreeMap::new(),
        buffered_events: Vec::new(),
        auto_reset_points: Vec::new(),
    }
}

fn pending_task(
    state: &WorkflowState,
    task_type: WorkflowTaskType,
    scheduled_event_id: i64,
    started_event_id: Option<i64>,
) -> PendingWorkflowTask {
    PendingWorkflowTask {
        advice: Default::default(),
        task_type,
        schedule_to_start_deadline: None,
        target_worker_deployment_version_changed: false,
        target_version_changed_enabled: false,
        target_deployment_version: None,
        logical_seq: LogicalTaskSeq(30),
        scheduled_event_id,
        scheduled_at: state.started_at,
        started_event_id,
        started_at: started_event_id.map(|_| state.started_at + Duration::seconds(1)),
        attempt: 1,
    }
}

/// A run whose attempt-1 workflow task is started (Scheduled 13, Started 14).
fn started(mut state: WorkflowState) -> WorkflowState {
    state.pending_workflow_task =
        Some(pending_task(&state, WorkflowTaskType::Normal, 13, Some(14)));
    state.next_workflow_task_seq = LogicalTaskSeq(31);
    state
}

/// A run whose workflow task is scheduled and not started (Scheduled 14).
fn scheduled(mut state: WorkflowState) -> WorkflowState {
    state.pending_workflow_task = Some(pending_task(&state, WorkflowTaskType::Normal, 14, None));
    state.next_workflow_task_seq = LogicalTaskSeq(31);
    state
}

/// A run whose speculative workflow task is scheduled at the id it reserved.
fn speculative(mut state: WorkflowState) -> WorkflowState {
    state.pending_workflow_task = Some(pending_task(
        &state,
        WorkflowTaskType::Speculative,
        15,
        None,
    ));
    state.next_workflow_task_seq = LogicalTaskSeq(31);
    state
}

fn signal(state: WorkflowState, index: usize, input: Payloads) -> Transition {
    BasicKernel
        .apply(
            LoadedRun::Existing(state),
            Command::Signal(SignalRequest {
                signal_name: "sig".into(),
                input,
                header: None,
                links: Vec::new(),
                request: request(&format!("signal-{index}")),
                now: now(),
            }),
        )
        .unwrap()
}

fn completion(state: &WorkflowState, commands: Vec<WorkflowCommand>) -> Command {
    let pending = state.pending_workflow_task.as_ref().unwrap();
    Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
        token: WorkflowTaskToken {
            run_key: state.run_key,
            logical_seq: pending.logical_seq,
            started_event_id: pending.started_event_id.unwrap(),
            attempt: pending.attempt,
            shard_epoch: ShardEpoch::ZERO,
        },
        client_discards_speculative_with_events: false,
        identity: WorkerIdentity("worker".into()),
        sdk_metadata: None,
        metering_metadata: None,
        worker_version: None,
        versioning_behavior: Default::default(),
        deployment_version: None,
        worker_deployment_name: None,
        sticky: None,
        commands,
        command_sizes: Vec::new(),
        force_new_workflow_task: false,
        limits: WorkflowTaskCompletionLimits::default(),
        delivered_update_ids: Vec::new(),
        request: RequestContext::unattributed(OffsetDateTime::UNIX_EPOCH),
        now: now(),
    })
}

fn schedule_activity(index: usize) -> WorkflowCommand {
    WorkflowCommand::ScheduleActivity {
        activity_id: format!("activity-{index}"),
        activity_type: "work".into(),
        task_queue: TaskQueueName(String::new()),
        input: payloads_of(4),
        header: None,
        request_eager_execution: false,
        retry_policy: None,
        deployment: None,
        build_id: None,
        schedule_to_close_timeout: None,
        schedule_to_start_timeout: None,
        start_to_close_timeout: Some(Duration::seconds(10)),
        heartbeat_timeout: None,
        priority: None,
    }
}

fn start_activity(state: WorkflowState, index: usize) -> Transition {
    BasicKernel
        .apply_activity_started(
            LoadedRun::Existing(state),
            ActivityStartRequest {
                activity_id: format!("activity-{index}"),
                identity: WorkerIdentity("worker".into()),
                principal: None,
                mode: ActivityStartMode::Poll,
                now: now(),
            },
        )
        .unwrap()
}

fn complete_activity(state: WorkflowState, index: usize, result: Payloads) -> Transition {
    BasicKernel
        .apply(
            LoadedRun::Existing(state),
            Command::ActivityResolved(ActivityResolvedRequest {
                activity_id: format!("activity-{index}"),
                resolution: ActivityResolution::Completed { result },
                worker_identity: Some(WorkerIdentity("worker".into())),
                request: request(&format!("complete-{index}")),
                now: now(),
            }),
        )
        .unwrap()
}

fn start_workflow_task(state: WorkflowState) -> Transition {
    let logical_seq = state.pending_workflow_task.as_ref().unwrap().logical_seq;
    BasicKernel
        .apply(
            LoadedRun::Existing(state),
            Command::WorkflowTaskStarted(StartWorkflowTaskRequest {
                advice_policy: ContinueAsNewAdvicePolicy::V1_31_0,
                logical_seq,
                worker_identity: WorkerIdentity("worker".into()),
                request_id: "start-wft".into(),
                history_size_bytes: 0,
                deployment_transition: None,
                deployment_transition_revision_number: None,
                target_version_changed_enabled: false,
                target_deployment_version: None,
                polled_task_queue: TaskQueueName("queue".into()),
                now: now(),
            }),
        )
        .unwrap()
}

/// A run whose `count` activities were started by a worker and whose workflow
/// task is started, so each activity's result buffers one event.
fn with_started_activities(count: usize) -> WorkflowState {
    let state = started(open_state());
    let commands = (0..count).map(schedule_activity).collect();
    let command = completion(&state, commands);
    let mut state = BasicKernel
        .apply(LoadedRun::Existing(state), command)
        .unwrap()
        .next_state;
    assert!(state.pending_workflow_task.is_none());
    for index in 0..count {
        let transition = start_activity(state, index);
        // Without a retry policy the start is written now, so the result
        // later buffers only its own event.
        assert_eq!(transition.history_events.len(), 1);
        state = transition.next_state;
    }
    let state = signal(state, 0, payloads_of(1)).next_state;
    let state = start_workflow_task(state).next_state;
    assert!(
        state
            .pending_workflow_task
            .as_ref()
            .unwrap()
            .started_event_id
            .is_some()
    );
    state
}

fn names(transition: &Transition) -> Vec<&'static str> {
    transition
        .history_events
        .iter()
        .map(|event| match &event.kind {
            HistoryEventKind::WorkflowExecutionSignaled { .. } => "Signaled",
            HistoryEventKind::WorkflowTaskScheduled { .. } => "WorkflowTaskScheduled",
            HistoryEventKind::WorkflowTaskCompleted { .. } => "WorkflowTaskCompleted",
            HistoryEventKind::WorkflowTaskFailed { .. } => "WorkflowTaskFailed",
            HistoryEventKind::ActivityTaskCompleted { .. } => "ActivityTaskCompleted",
            HistoryEventKind::NexusOperationStarted { .. } => "NexusOperationStarted",
            HistoryEventKind::NexusOperationCompleted { .. } => "NexusOperationCompleted",
            _ => "other",
        })
        .collect()
}

fn force_closed(transition: &Transition) -> bool {
    transition.history_events.iter().any(|event| {
        matches!(
            event.kind,
            HistoryEventKind::WorkflowTaskFailed {
                failure_cause: WorkflowTaskFailedCause::ForceCloseCommand,
                ..
            }
        )
    })
}

#[test]
fn a_signal_while_the_task_is_scheduled_is_numbered_at_close() {
    let transition = signal(scheduled(open_state()), 0, payloads_of(4));
    assert_eq!(names(&transition), ["Signaled"]);
    assert_eq!(transition.events_numbered_at_close, 1);
}

#[test]
fn a_signal_that_schedules_a_task_is_numbered_while_handling_it() {
    let transition = signal(open_state(), 0, payloads_of(4));
    assert_eq!(names(&transition), ["Signaled", "WorkflowTaskScheduled"]);
    assert_eq!(transition.events_numbered_at_close, 0);
}

#[test]
fn a_buffered_signal_writes_nothing_to_number() {
    let transition = signal(started(open_state()), 0, payloads_of(4));
    assert!(transition.history_events.is_empty());
    assert_eq!(transition.next_state.buffered_events.len(), 1);
    assert_eq!(transition.events_numbered_at_close, 0);
}

#[test]
fn an_activity_result_is_numbered_at_close_unless_it_schedules_a_task() {
    let state = started(open_state());
    let command = completion(&state, vec![schedule_activity(0)]);
    let state = BasicKernel
        .apply(LoadedRun::Existing(state), command)
        .unwrap()
        .next_state;
    let state = start_activity(state, 0).next_state;

    // With no task pending, the result schedules one while it is handled.
    let unscheduled = complete_activity(state.clone(), 0, payloads_of(4));
    assert_eq!(
        names(&unscheduled),
        ["ActivityTaskCompleted", "WorkflowTaskScheduled"]
    );
    assert_eq!(unscheduled.events_numbered_at_close, 0);

    // With a task already scheduled, the result is flushed when the write
    // finishes.
    let state = signal(state, 0, payloads_of(4)).next_state;
    let pending = complete_activity(state, 0, payloads_of(4));
    assert_eq!(names(&pending), ["ActivityTaskCompleted"]);
    assert_eq!(pending.events_numbered_at_close, 1);
}

#[test]
fn a_nexus_operation_result_schedules_its_task_at_close() {
    let mut state = open_state();
    state.pending_nexus_operations.insert(
        "operation".into(),
        PendingNexusOperation {
            operation_id: "operation".into(),
            scheduled_event_id: 12,
            endpoint: "endpoint".into(),
            service: "service".into(),
            operation: "operation".into(),
            schedule_to_close_timeout: None,
            schedule_to_start_timeout: None,
            start_to_close_timeout: None,
            scheduled_at: OffsetDateTime::UNIX_EPOCH,
            started: false,
            started_at: None,
            attempt: 0,
            last_attempt_failure: None,
            next_attempt_at: None,
            operation_token: String::new(),
            input: Default::default(),
            cancellation: None,
        },
    );
    let transition = BasicKernel
        .apply(
            LoadedRun::Existing(state),
            Command::NexusOperationResolved(NexusOperationResolvedRequest {
                operation_id: "operation".into(),
                scheduled_event_id: 12,
                resolution: NexusResolution::Completed {
                    result: payloads_of(4),
                    links: Vec::new(),
                },
                now: now(),
            }),
        )
        .unwrap();
    assert_eq!(
        names(&transition).last(),
        Some(&"WorkflowTaskScheduled"),
        "{:?}",
        names(&transition)
    );
    assert_eq!(
        transition.events_numbered_at_close as usize,
        transition.history_events.len()
    );
}

#[test]
fn a_speculative_task_converted_for_a_signal_is_numbered_at_close() {
    let transition = signal(speculative(open_state()), 0, payloads_of(4));
    assert_eq!(names(&transition), ["WorkflowTaskScheduled", "Signaled"]);
    assert_eq!(transition.events_numbered_at_close, 2);
}

#[test]
fn a_completion_that_flushes_buffered_events_is_numbered_while_handling_it() {
    let state = signal(started(open_state()), 0, payloads_of(4)).next_state;
    let command = completion(&state, Vec::new());
    let transition = BasicKernel
        .apply(LoadedRun::Existing(state), command)
        .unwrap();
    assert_eq!(
        names(&transition),
        ["WorkflowTaskCompleted", "Signaled", "WorkflowTaskScheduled"]
    );
    assert_eq!(transition.events_numbered_at_close, 0);
}

#[test]
fn a_force_close_for_buffered_events_is_numbered_at_close() {
    let mut state = started(open_state());
    for index in 0..MAXIMUM_BUFFERED_EVENTS_BATCH {
        state = signal(state, index, payloads_of(4)).next_state;
    }
    let transition = signal(state, MAXIMUM_BUFFERED_EVENTS_BATCH, payloads_of(4));
    assert!(force_closed(&transition));
    assert_eq!(
        transition.history_events.len(),
        1 + MAXIMUM_BUFFERED_EVENTS_BATCH + 1 + 1
    );
    assert_eq!(
        transition.events_numbered_at_close as usize,
        transition.history_events.len()
    );
}

#[test]
fn buffered_events_over_the_size_limit_force_close_the_task() {
    // Two signals of 1 MiB each carry more than 2 MiB of payloads.
    let first = signal(started(open_state()), 0, payloads_of(1024 * 1024));
    assert!(!force_closed(&first));
    let second = signal(first.next_state, 1, payloads_of(1024 * 1024));
    assert!(force_closed(&second));
    assert_eq!(
        names(&second),
        [
            "WorkflowTaskFailed",
            "Signaled",
            "Signaled",
            "WorkflowTaskScheduled"
        ]
    );
    assert!(second.next_state.buffered_events.is_empty());
}

#[test]
fn activity_results_over_the_count_limit_force_close_the_task() {
    let mut state = with_started_activities(MAXIMUM_BUFFERED_EVENTS_BATCH + 1);
    for index in 0..MAXIMUM_BUFFERED_EVENTS_BATCH {
        let transition = complete_activity(state, index, payloads_of(4));
        assert!(!force_closed(&transition));
        state = transition.next_state;
    }
    let transition = complete_activity(state, MAXIMUM_BUFFERED_EVENTS_BATCH, payloads_of(4));
    assert!(force_closed(&transition));
    assert!(transition.next_state.buffered_events.is_empty());
}

#[test]
fn an_activity_start_leaves_the_buffered_limits_to_the_next_transition() {
    // An activity scheduled by one task, and a second task started.
    let state = started(open_state());
    let command = completion(&state, vec![schedule_activity(0)]);
    let state = BasicKernel
        .apply(LoadedRun::Existing(state), command)
        .unwrap()
        .next_state;
    let state = signal(state, 0, payloads_of(4)).next_state;
    let mut state = start_workflow_task(state).next_state;
    for index in 1..=MAXIMUM_BUFFERED_EVENTS_BATCH {
        state = signal(state, index, payloads_of(4)).next_state;
    }
    assert_eq!(state.buffered_events.len(), MAXIMUM_BUFFERED_EVENTS_BATCH);

    // The runtime commits the start outside the lane, so its buffered event
    // waits for the next transition to force-close the task.
    let start = start_activity(state, 0);
    assert!(start.history_events.is_empty());
    assert_eq!(
        start.next_state.buffered_events.len(),
        MAXIMUM_BUFFERED_EVENTS_BATCH + 1
    );
    let next = signal(
        start.next_state,
        MAXIMUM_BUFFERED_EVENTS_BATCH + 1,
        payloads_of(4),
    );
    assert!(force_closed(&next));
}

/// Activities the property's runs can complete.
const ACTIVITIES: usize = 40;

fn base_with_activities() -> &'static WorkflowState {
    static BASE: OnceLock<WorkflowState> = OnceLock::new();
    BASE.get_or_init(|| with_started_activities(ACTIVITIES))
}

#[derive(Clone, Debug)]
enum Step {
    Signal(usize),
    ActivityResult(usize),
}

fn arb_size() -> impl Strategy<Value = usize> {
    prop_oneof![8 => 0..2048usize, 2 => 0..=600_000usize]
}

fn arb_steps() -> impl Strategy<Value = Vec<Step>> {
    prop::collection::vec(
        (prop::bool::weighted(0.3), arb_size()).prop_map(|(activity, size)| {
            if activity {
                Step::ActivityResult(size)
            } else {
                Step::Signal(size)
            }
        }),
        1..130,
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    // Feature: run-growth-limits, Property 3: Buffered events are bounded on every path
    #[test]
    fn property_3_buffered_events_are_bounded_on_every_path(steps in arb_steps()) {
        let mut state = base_with_activities().clone();
        let mut next_activity = 0;
        let (mut count, mut size) = (0usize, 0usize);
        for (index, step) in steps.into_iter().enumerate() {
            let transition = match step {
                Step::ActivityResult(bytes) if next_activity < ACTIVITIES => {
                    next_activity += 1;
                    size += payloads_encoded_len(&payloads_of(bytes));
                    complete_activity(state, next_activity - 1, payloads_of(bytes))
                }
                Step::ActivityResult(bytes) | Step::Signal(bytes) => {
                    size += payloads_encoded_len(&payloads_of(bytes));
                    signal(state, index + 1, payloads_of(bytes))
                }
            };
            count += 1;
            let over = count > MAXIMUM_BUFFERED_EVENTS_BATCH
                || size > MAXIMUM_BUFFERED_EVENTS_SIZE_IN_BYTES;
            prop_assert_eq!(force_closed(&transition), over, "step {}", index);
            if over {
                prop_assert!(transition.next_state.buffered_events.is_empty());
                let pending = transition.next_state.pending_workflow_task.as_ref().unwrap();
                prop_assert!(pending.started_event_id.is_none());
                prop_assert_eq!(
                    transition.events_numbered_at_close as usize,
                    transition.history_events.len()
                );
                return Ok(());
            }
            prop_assert!(transition.history_events.is_empty());
            prop_assert_eq!(transition.next_state.buffered_events.len(), count);
            state = transition.next_state;
        }
    }
}
