//! `signal-update-limits`: the kernel counts a run's signals once each and
//! refuses one at the signal limit, checks an update's in-flight count, total
//! and in-flight payload in Temporal v1.31.0's order, refuses a worker's
//! message about an update the run doesn't hold at the total limit, and
//! rebuilds both counts when it replays a copied history.
// Integration test: unwrap is idiomatic in test code (root AGENTS.md §1).
#![allow(clippy::unwrap_used)]

use std::collections::{BTreeMap, HashSet};

use proptest::prelude::*;
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    BasicKernel, Command, ContinueAsNewAdvicePolicy, HistoryEvent, LoadedRun, PendingUpdate,
    PendingWorkflowTask, Reject, ReplayContext, SignalRequest, SignalWithStartRequest,
    StartRequest, StartWorkflowTaskRequest, Transition, UpdateProtocolBody, UpdateRequest,
    WorkflowCommand, WorkflowState, WorkflowTaskCompletedRequest, WorkflowTaskCompletionLimits,
    WorkflowTaskFailedCause, WorkflowTaskFailedRequest, WorkflowTaskType,
    event::HistoryEventKind,
    kernel::Kernel,
    limits::{MAXIMUM_SIGNALS_PER_EXECUTION, UpdateLimit, UpdateLimitExceeded, UpdateLimits},
};
use tokeira_types::{
    ExecutionStatus, LogicalTaskSeq, Memo, NamespaceId, Payload, Payloads, RequestContext,
    RequestId, RunId, RunKey, SearchAttributes, ShardEpoch, TaskQueueName, TransitionSeq,
    WorkerIdentity, WorkflowId, WorkflowTaskToken, WorkflowType,
};

fn now() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()
}

fn payloads(data: &str) -> Payloads {
    Payloads(vec![Payload::new(data.as_bytes().to_vec())])
}

fn request(id: &str) -> RequestContext {
    RequestContext {
        request_id: RequestId(id.into()),
        caller_identity: Some("tester".into()),
        principal: None,
        received_at: now(),
    }
}

/// An open run with 14 events, no pending workflow task and `signal_count`
/// recorded signals.
fn open_state(signal_count: u64) -> WorkflowState {
    let now = now();
    WorkflowState {
        used_worker_deployment_versions: Some(Vec::new()),
        completed_update_count: 0,
        signal_count,
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

/// `state` with an attempt-1 workflow task scheduled at 13 and, when
/// `started`, started at 14.
fn with_task(mut state: WorkflowState, started: bool) -> WorkflowState {
    state.pending_workflow_task = Some(PendingWorkflowTask {
        advice: Default::default(),
        task_type: WorkflowTaskType::Normal,
        schedule_to_start_deadline: None,
        target_worker_deployment_version_changed: false,
        target_version_changed_enabled: false,
        target_deployment_version: None,
        logical_seq: LogicalTaskSeq(30),
        scheduled_event_id: 13,
        scheduled_at: state.started_at,
        started_event_id: started.then_some(14),
        started_at: started.then(|| state.started_at + Duration::seconds(1)),
        attempt: 1,
    });
    state.next_workflow_task_seq = LogicalTaskSeq(31);
    state
}

fn signal_command(index: usize) -> Command {
    Command::Signal(SignalRequest {
        signal_name: "sig".into(),
        input: payloads("signal"),
        header: None,
        links: Vec::new(),
        request: request(&format!("signal-{index}")),
        now: now(),
    })
}

fn start_request() -> StartRequest {
    let run_id = RunId::new();
    let namespace_id = NamespaceId::new();
    let workflow_id = WorkflowId("limits-workflow".into());
    StartRequest {
        advice_policy: ContinueAsNewAdvicePolicy::V1_31_0,
        initiator: None,
        run_key: RunKey::derive(namespace_id, &workflow_id, run_id),
        namespace_id,
        workflow_id,
        run_id,
        workflow_type: WorkflowType("wf".into()),
        task_queue: TaskQueueName("queue".into()),
        deployment: None,
        build_id: None,
        versioning_override: None,
        workflow_start_delay: None,
        completion_callbacks: Vec::new(),
        user_metadata: None,
        links: Vec::new(),
        on_conflict_options: None,
        priority: None,
        input: payloads("input"),
        header: None,
        memo: Memo::default(),
        search_attributes: SearchAttributes::default(),
        workflow_execution_timeout: None,
        workflow_run_timeout: None,
        workflow_task_timeout: Duration::seconds(10),
        retry_policy: None,
        conflict_policy: tokeira_kernel::WorkflowIdConflictPolicy::Fail,
        reuse_policy: tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate,
        attempt: 1,
        continued_execution_run_id: None,
        first_execution_run_id: Some(run_id),
        parent_run_key: None,
        parent_workflow_id: None,
        parent_run_id: None,
        parent_namespace_id: None,
        parent_namespace_name: None,
        parent_initiated_event_id: 0,
        root_workflow_id: None,
        root_run_id: None,
        original_execution_run_id: Some(run_id),
        continued_failure: None,
        last_completion_result: None,
        first_run_started_at: None,
        request: request("start"),
        now: now(),
        client_cron_schedule: None,
        cron_schedule: None,
        eager_execution_accepted: false,
        reserved_poller_identity: None,
        inherited_versioning_info: None,
    }
}

/// Start the run's pending workflow task, as a worker's poll does.
fn start_task(state: &WorkflowState, index: usize) -> Transition {
    let logical_seq = state.pending_workflow_task.as_ref().unwrap().logical_seq;
    BasicKernel
        .apply(
            LoadedRun::Existing(state.clone()),
            Command::WorkflowTaskStarted(StartWorkflowTaskRequest {
                advice_policy: ContinueAsNewAdvicePolicy::V1_31_0,
                logical_seq,
                worker_identity: WorkerIdentity("worker".into()),
                request_id: format!("start-task-{index}"),
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

/// A completion of the run's started workflow task, with `held_updates` as the
/// lane would report them.
fn completion(
    state: &WorkflowState,
    commands: Vec<WorkflowCommand>,
    held_updates: usize,
    total_updates: usize,
) -> Command {
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
        limits: WorkflowTaskCompletionLimits {
            total_updates,
            ..WorkflowTaskCompletionLimits::default()
        },
        delivered_update_ids: Vec::new(),
        held_updates,
        request: RequestContext::unattributed(OffsetDateTime::UNIX_EPOCH),
        now: now(),
    })
}

fn update_command(
    update_id: &str,
    limits: UpdateLimits,
    held_updates: usize,
    in_flight_request_bytes: u64,
    request_bytes: u64,
) -> Command {
    Command::Update(UpdateRequest {
        update_id: update_id.into(),
        update_name: "handler".into(),
        input: payloads("update"),
        request: request(&format!("update-{update_id}")),
        now: now(),
        limits,
        request_bytes,
        held_updates,
        in_flight_request_bytes,
    })
}

fn message(index: usize, body: UpdateProtocolBody) -> WorkflowCommand {
    WorkflowCommand::ProtocolMessage {
        message_id: format!("message-{index}"),
        body,
    }
}

fn accepted(update_id: &str, update_name: &str) -> UpdateProtocolBody {
    UpdateProtocolBody::Accepted {
        update_id: update_id.into(),
        update_name: update_name.into(),
        input: payloads("update"),
        sequencing_event_id: 1,
    }
}

fn replay(state: &WorkflowState, history: &[HistoryEvent]) -> WorkflowState {
    BasicKernel
        .replay_history_prefix(
            ReplayContext {
                run_key: state.run_key,
                namespace_id: state.namespace_id,
                workflow_id: state.workflow_id.clone(),
                run_id: state.run_id,
                deployment: None,
                build_id: None,
                parent_run_key: None,
                parent_workflow_id: None,
                first_run_started_at: state.first_run_started_at,
            },
            history,
        )
        .unwrap()
}

fn signaled_events<'a>(kinds: impl IntoIterator<Item = &'a HistoryEventKind>) -> u64 {
    kinds
        .into_iter()
        .filter(|kind| matches!(kind, HistoryEventKind::WorkflowExecutionSignaled { .. }))
        .count() as u64
}

/// A limit's offset from the figure it bounds, or `None` for a disabled limit.
fn limit_offset(offsets: std::ops::Range<i64>) -> impl Strategy<Value = Option<i64>> {
    prop_oneof![1 => Just(None), 5 => offsets.prop_map(Some)]
}

/// The limit `offset` from `figure`, at least 1; `None` disables it.
fn limit_near(figure: u64, offset: Option<i64>) -> usize {
    offset.map_or(0, |offset| {
        usize::try_from((figure as i64 + offset).max(1)).unwrap()
    })
}

fn update_refusal(result: Result<Transition, Reject>) -> Option<UpdateLimit> {
    match result {
        Err(Reject::UpdateLimitExceeded(UpdateLimitExceeded { limit, .. })) => Some(limit),
        _ => None,
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    // Feature: signal-update-limits, Property 1: The signal count is the run's recorded signals
    #[test]
    fn the_signal_count_is_the_runs_recorded_signals(
        steps in prop::collection::vec(0u8..3, 1..40),
    ) {
        let start = BasicKernel
            .apply(LoadedRun::Absent, Command::Start(start_request()))
            .unwrap();
        let mut history = start.history_events.to_vec();
        let mut state = start.next_state;
        for (index, step) in steps.into_iter().enumerate() {
            let pending = state.pending_workflow_task.clone();
            let transition = match (step, pending) {
                (0, _) => BasicKernel
                    .apply(LoadedRun::Existing(state.clone()), signal_command(index))
                    .unwrap(),
                (1, Some(task)) if task.started_event_id.is_none() => start_task(&state, index),
                (2, Some(task)) if task.started_event_id.is_some() => BasicKernel
                    .apply(
                        LoadedRun::Existing(state.clone()),
                        completion(&state, Vec::new(), 0, 0),
                    )
                    .unwrap(),
                _ => continue,
            };
            history.extend(transition.history_events.iter().cloned());
            state = transition.next_state;
            // Each signal counts once, when admitted: in history or still
            // buffered, never again when a buffered signal is flushed.
            let buffered = state.buffered_events.iter().map(|event| &event.kind);
            prop_assert_eq!(
                state.signal_count,
                signaled_events(history.iter().map(|event| &event.kind))
                    + signaled_events(buffered)
            );
        }
        // A reset run's replay counts the copied history's signals.
        let replayed = replay(&state, &history);
        prop_assert_eq!(
            replayed.signal_count,
            signaled_events(history.iter().map(|event| &event.kind))
        );
    }

    // Feature: signal-update-limits, Property 2: A client's signal is answered as v1.31.0 answers it
    #[test]
    fn a_signal_is_refused_exactly_at_the_limit(
        offset in -3i64..=3,
        started in any::<bool>(),
        closed in any::<bool>(),
    ) {
        let count = MAXIMUM_SIGNALS_PER_EXECUTION.saturating_add_signed(offset);
        let mut state = with_task(open_state(count), started);
        if closed {
            state.pending_workflow_task = None;
            state.status = ExecutionStatus::Completed;
        }
        let result = BasicKernel.apply(LoadedRun::Existing(state), signal_command(0));
        if closed {
            prop_assert!(matches!(result, Err(Reject::RunClosed(_))));
        } else if count >= MAXIMUM_SIGNALS_PER_EXECUTION {
            prop_assert_eq!(result.unwrap_err(), Reject::SignalLimitExceeded);
        } else {
            prop_assert_eq!(result.unwrap().next_state.signal_count, count + 1);
        }
    }

    // Feature: signal-update-limits, Property 4: Update admission matches v1.31.0's
    #[test]
    fn update_admission_matches_v1_31_0(
        accepted_updates in 0usize..14,
        held_updates in 0usize..14,
        unheld_updates in 0usize..14,
        completed in 0u32..2_100,
        in_flight_request_bytes in 0u64..(24 << 20),
        request_bytes in 0u64..(5 << 20),
        in_flight_offset in limit_offset(-1..4),
        total_offset in limit_offset(-1..4),
        payload_offset in limit_offset(-1..2),
        duplicate in any::<bool>(),
    ) {
        let mut state = open_state(0);
        state.completed_update_count = completed;
        for index in 0..accepted_updates {
            let update_id = format!("accepted-{index}");
            state.pending_updates.insert(
                update_id.clone(),
                PendingUpdate { update_id, accepted_event_id: 3, name: "handler".into() },
            );
        }
        // Admitted ids the lane reports held, and ones whose requests a restart
        // lost; only the held count reaches the kernel.
        for index in 0..held_updates + unheld_updates {
            state.admitted_updates.insert(format!("admitted-{index}"));
        }
        // Each limit is disabled, or near the figure it bounds, so that each
        // check is tried on both sides of its boundary.
        let in_flight = held_updates + accepted_updates;
        let limits = UpdateLimits {
            in_flight: limit_near(in_flight as u64, in_flight_offset),
            in_flight_payloads: limit_near(in_flight_request_bytes + request_bytes, payload_offset),
            total: limit_near(in_flight as u64 + u64::from(completed), total_offset),
        };
        let update_id = if duplicate { "admitted-0" } else { "new" };
        let duplicate = duplicate && held_updates + unheld_updates > 0;
        let result = BasicKernel.apply(
            LoadedRun::Existing(state),
            update_command(update_id, limits, held_updates, in_flight_request_bytes, request_bytes),
        );
        let expected = if duplicate {
            None
        } else if limits.in_flight > 0 && in_flight >= limits.in_flight {
            Some(UpdateLimit::InFlight)
        } else if limits.total > 0 && in_flight + completed as usize >= limits.total {
            Some(UpdateLimit::Total)
        } else if limits.in_flight_payloads > 0
            && in_flight_request_bytes + request_bytes >= limits.in_flight_payloads as u64
        {
            Some(UpdateLimit::InFlightPayloads)
        } else {
            None
        };
        if duplicate {
            // A request for an update the run holds joins it: never refused for
            // a limit.
            prop_assert_eq!(result.unwrap_err(), Reject::DuplicateUpdateId(update_id.into()));
        } else {
            match expected {
                Some(limit) => prop_assert_eq!(update_refusal(result), Some(limit)),
                None => {
                    let next = result.unwrap().next_state;
                    prop_assert!(next.admitted_updates.contains(update_id));
                }
            }
        }
    }

    // Feature: signal-update-limits, Property 5: Updates are counted as v1.31.0 counts them
    #[test]
    fn updates_are_counted_as_v1_31_0_counts_them(
        rounds in prop::collection::vec(0u8..3, 1..8),
    ) {
        let start = BasicKernel
            .apply(LoadedRun::Absent, Command::Start(start_request()))
            .unwrap();
        let mut history = start.history_events.to_vec();
        let mut state = start.next_state;
        // Settle the first workflow task so each round's update has its own.
        let started = start_task(&state, 0);
        history.extend(started.history_events.iter().cloned());
        state = started.next_state;
        let completed_task = BasicKernel
            .apply(LoadedRun::Existing(state.clone()), completion(&state, Vec::new(), 0, 0))
            .unwrap();
        history.extend(completed_task.history_events.iter().cloned());
        state = completed_task.next_state;
        let disabled = UpdateLimits { in_flight: 0, in_flight_payloads: 0, total: 0 };
        let mut completed = 0u32;
        for (index, outcome) in rounds.into_iter().enumerate() {
            let update_id = format!("update-{index}");
            let admitted = BasicKernel
                .apply(LoadedRun::Existing(state.clone()), update_command(&update_id, disabled, 0, 0, 0))
                .unwrap();
            history.extend(admitted.history_events.iter().cloned());
            state = admitted.next_state;
            let started = start_task(&state, index + 1);
            history.extend(started.history_events.iter().cloned());
            state = started.next_state;
            let commands = match outcome {
                // Accepted and completed with a result, or with a failure:
                // both count as completed.
                0 | 1 => vec![
                    message(2 * index, accepted(&update_id, "handler")),
                    message(2 * index + 1, UpdateProtocolBody::Completed {
                        update_id: update_id.clone(),
                        result: payloads("result"),
                        failure: (outcome == 1).then(|| Payload::new(b"failed".to_vec())),
                    }),
                ],
                // Rejected: never counts.
                _ => vec![message(2 * index, UpdateProtocolBody::Rejected {
                    update_id: update_id.clone(),
                    failure: Payload::new(b"rejected".to_vec()),
                })],
            };
            let finished = BasicKernel
                .apply(LoadedRun::Existing(state.clone()), completion(&state, commands, 0, 0))
                .unwrap();
            history.extend(finished.history_events.iter().cloned());
            state = finished.next_state;
            if outcome < 2 {
                completed += 1;
            }
            prop_assert_eq!(state.completed_update_count, completed);
            prop_assert!(state.admitted_updates.is_empty() && state.pending_updates.is_empty());
        }
        // A reset run counts the completions in the history it copies.
        prop_assert_eq!(replay(&state, &history).completed_update_count, completed);
    }

    // Feature: signal-update-limits, Property 6: Resurrecting an update respects the total limit
    #[test]
    fn resurrection_respects_the_total_limit(
        accepted_updates in 0usize..6,
        held_updates in 0usize..6,
        completed in 0u32..8,
        total_limit in prop_oneof![Just(0usize), 1usize..20],
        kind in 0u8..4,
    ) {
        let mut state = with_task(open_state(0), true);
        state.completed_update_count = completed;
        for index in 0..accepted_updates {
            let update_id = format!("accepted-{index}");
            state.pending_updates.insert(
                update_id.clone(),
                PendingUpdate { update_id, accepted_event_id: 3, name: "handler".into() },
            );
        }
        let body = match kind {
            0 => accepted("ghost", "handler"),
            1 => accepted("ghost", ""),
            2 => UpdateProtocolBody::Rejected {
                update_id: "ghost".into(),
                failure: Payload::new(b"rejected".to_vec()),
            },
            _ => UpdateProtocolBody::Completed {
                update_id: "ghost".into(),
                result: payloads("result"),
                failure: None,
            },
        };
        let command = completion(&state, vec![message(0, body)], held_updates, total_limit);
        let result = BasicKernel.apply(LoadedRun::Existing(state), command);
        let at_total = total_limit > 0
            && held_updates + accepted_updates + completed as usize >= total_limit;
        if at_total {
            prop_assert_eq!(update_refusal(result), Some(UpdateLimit::Total));
        } else {
            match kind {
                0 => prop_assert!(result.unwrap().next_state.pending_updates.contains_key("ghost")),
                2 => prop_assert!(result.is_ok()),
                _ => prop_assert!(
                    matches!(result, Err(Reject::BadUpdateMessage { not_found: true, .. })),
                    "a message the run can't re-admit an update from is not found"
                ),
            }
        }
    }
}

/// Two re-admissions in one completion each count in flight, as v1.31.0's
/// registry holds the first while it handles the second.
#[test]
fn each_resurrection_counts_toward_the_next() {
    let state = with_task(open_state(0), true);
    let commands = vec![
        message(0, accepted("ghost-1", "handler")),
        message(1, accepted("ghost-2", "handler")),
    ];
    let refused = BasicKernel.apply(
        LoadedRun::Existing(state.clone()),
        completion(&state, commands.clone(), 0, 1),
    );
    assert_eq!(update_refusal(refused), Some(UpdateLimit::Total));
    let admitted = BasicKernel
        .apply(
            LoadedRun::Existing(state.clone()),
            completion(&state, commands, 0, 2),
        )
        .unwrap();
    assert_eq!(admitted.next_state.pending_updates.len(), 2);
}

/// A signal-with-start that starts a run counts its own signal, and the start's
/// count is never refused.
#[test]
fn signal_with_start_counts_its_own_signal() {
    let start = start_request();
    let transition = BasicKernel
        .apply(
            LoadedRun::Absent,
            Command::SignalWithStart(SignalWithStartRequest {
                advice_policy: ContinueAsNewAdvicePolicy::V1_31_0,
                client_cron_schedule: None,
                conflict_policy: tokeira_kernel::WorkflowIdConflictPolicy::UseExisting,
                reuse_policy: tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate,
                run_key: start.run_key,
                namespace_id: start.namespace_id,
                workflow_id: start.workflow_id,
                run_id: start.run_id,
                workflow_type: start.workflow_type,
                task_queue: start.task_queue,
                deployment: None,
                build_id: None,
                versioning_override: None,
                input: start.input,
                signal_name: "sig".into(),
                signal_input: payloads("signal"),
                header: None,
                links: Vec::new(),
                memo: Memo::default(),
                search_attributes: SearchAttributes::default(),
                workflow_execution_timeout: None,
                workflow_run_timeout: None,
                workflow_task_timeout: Duration::seconds(10),
                retry_policy: None,
                attempt: 1,
                first_execution_run_id: start.first_execution_run_id,
                original_execution_run_id: start.original_execution_run_id,
                parent_run_key: None,
                parent_workflow_id: None,
                parent_run_id: None,
                parent_namespace_id: None,
                parent_namespace_name: None,
                parent_initiated_event_id: 0,
                root_workflow_id: None,
                root_run_id: None,
                last_completion_result: None,
                first_run_started_at: None,
                continued_execution_run_id: None,
                continued_failure: None,
                initiator: None,
                cron_schedule: None,
                workflow_start_delay: None,
                user_metadata: None,
                priority: None,
                request: request("signal-with-start"),
                now: now(),
            }),
        )
        .unwrap();
    assert_eq!(transition.next_state.signal_count, 1);
}

/// A reset's reapplied signals count, beyond the limit, and are never refused.
#[test]
fn reset_reapplied_signals_count_and_are_never_refused() {
    let state = with_task(open_state(MAXIMUM_SIGNALS_PER_EXECUTION), true);
    let signaled = HistoryEventKind::WorkflowExecutionSignaled {
        signal_name: "sig".into(),
        input: payloads("signal"),
        header: None,
        links: Vec::new(),
        request_id: "reapplied".into(),
        identity: Some("tester".into()),
    };
    let pending = state.pending_workflow_task.as_ref().unwrap();
    let transition = BasicKernel
        .apply(
            LoadedRun::Existing(state.clone()),
            Command::WorkflowTaskFailed(WorkflowTaskFailedRequest {
                logical_seq: pending.logical_seq,
                started_event_id: pending.started_event_id.unwrap(),
                failure_cause: WorkflowTaskFailedCause::ResetWorkflow,
                failure_details: None,
                worker_identity: WorkerIdentity("reset".into()),
                request: request("reset"),
                now: now(),
                reset_reapply: vec![signaled.clone(), signaled.clone(), signaled],
                history_size_bytes: 0,
                advice_policy: ContinueAsNewAdvicePolicy::V1_31_0,
                terminate_reason: None,
            }),
        )
        .unwrap();
    assert_eq!(
        transition.next_state.signal_count,
        MAXIMUM_SIGNALS_PER_EXECUTION + 3
    );
}

/// A replayed history counts every copied signal, past the limit too.
#[test]
fn replay_counts_copied_signals_past_the_limit() {
    let start = BasicKernel
        .apply(LoadedRun::Absent, Command::Start(start_request()))
        .unwrap();
    let state = start.next_state.clone();
    let signal = BasicKernel
        .apply(LoadedRun::Existing(state.clone()), signal_command(0))
        .unwrap();
    let template = signal
        .history_events
        .iter()
        .find(|event| {
            matches!(
                event.kind,
                HistoryEventKind::WorkflowExecutionSignaled { .. }
            )
        })
        .unwrap()
        .clone();
    let mut history = start.history_events.to_vec();
    let copied = MAXIMUM_SIGNALS_PER_EXECUTION + 2;
    let mut event_id = history.last().unwrap().event_id;
    for _ in 0..copied {
        event_id += 1;
        let mut event = template.clone();
        event.event_id = event_id;
        history.push(event);
    }
    assert_eq!(replay(&state, &history).signal_count, copied);
}
