//! `admitted-updates-after-restart` in the kernel. Once the runtime has
//! forgotten a run's lost updates, the run keeps only the admitted updates it
//! can deliver; a follow-up speculative task carries something; in flight is
//! held, history-admitted and accepted; and history admission follows the run
//! through its reset, its replay, the worker's answers and its close.
// Integration test: unwrap is idiomatic in test code (root AGENTS.md §1).
#![allow(clippy::unwrap_used)]

use std::collections::{BTreeMap, BTreeSet, HashSet};

use proptest::prelude::*;
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    BasicKernel, Command, ContinueAsNewAdvicePolicy, HistoryEvent, LoadedRun, PendingUpdate,
    PendingWorkflowTask, Reject, ReplayContext, StartRequest, StartWorkflowTaskRequest,
    TerminateRequest, Transition, UpdateProtocolBody, UpdateRequest, WorkflowCommand,
    WorkflowState, WorkflowTaskCompletedRequest, WorkflowTaskCompletionLimits,
    WorkflowTaskFailedCause, WorkflowTaskFailedRequest, WorkflowTaskType,
    event::HistoryEventKind,
    forget_lost_updates,
    kernel::Kernel,
    limits::{UpdateLimit, UpdateLimitExceeded, UpdateLimits},
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

/// An open run with 14 events and no pending workflow task.
fn open_state() -> WorkflowState {
    let now = now();
    WorkflowState {
        used_worker_deployment_versions: Some(Vec::new()),
        completed_update_count: 0,
        signal_count: 0,
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
        history_admitted_updates: BTreeSet::new(),
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

/// `state` with an attempt-1 workflow task of `task_type` scheduled at 13 and,
/// when `started`, started at 14.
fn with_task(
    mut state: WorkflowState,
    task_type: WorkflowTaskType,
    started: bool,
) -> WorkflowState {
    state.pending_workflow_task = Some(PendingWorkflowTask {
        advice: Default::default(),
        task_type,
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

/// `state` holding `held` and `history_admitted` as its admitted updates, the
/// latter recorded by WorkflowExecutionUpdateAdmitted events, and `accepted`.
fn with_updates(
    mut state: WorkflowState,
    held: &BTreeSet<String>,
    history_admitted: &BTreeSet<String>,
    accepted: usize,
) -> WorkflowState {
    state.admitted_updates = held.iter().chain(history_admitted).cloned().collect();
    state.history_admitted_updates = history_admitted.clone();
    for index in 0..accepted {
        let update_id = format!("accepted-{index}");
        state.pending_updates.insert(
            update_id.clone(),
            PendingUpdate {
                update_id,
                accepted_event_id: 3,
                name: "handler".into(),
            },
        );
    }
    state
}

fn start_request() -> StartRequest {
    let run_id = RunId::new();
    let namespace_id = NamespaceId::new();
    let workflow_id = WorkflowId("restarted-workflow".into());
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

/// A completion of the run's started workflow task, which delivered
/// `delivered` as protocol messages.
fn completion(
    state: &WorkflowState,
    commands: Vec<WorkflowCommand>,
    delivered: Vec<String>,
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
        delivered_update_ids: delivered,
        request: RequestContext::unattributed(OffsetDateTime::UNIX_EPOCH),
        now: now(),
    })
}

fn update_command(update_id: &str, limits: UpdateLimits, readmit: bool) -> Command {
    Command::Update(UpdateRequest {
        update_id: update_id.into(),
        update_name: "handler".into(),
        input: payloads("update"),
        request: request(&format!("update-{update_id}")),
        now: now(),
        limits,
        request_bytes: 0,
        in_flight_request_bytes: 0,
        readmit,
    })
}

const UNLIMITED: UpdateLimits = UpdateLimits {
    in_flight: 0,
    in_flight_payloads: 0,
    total: 0,
};

fn message(index: usize, body: UpdateProtocolBody) -> WorkflowCommand {
    WorkflowCommand::ProtocolMessage {
        message_id: format!("message-{index}"),
        body,
    }
}

fn accepted(update_id: &str) -> UpdateProtocolBody {
    UpdateProtocolBody::Accepted {
        update_id: update_id.into(),
        update_name: "handler".into(),
        input: payloads("update"),
        sequencing_event_id: 1,
    }
}

fn rejected(update_id: &str) -> UpdateProtocolBody {
    UpdateProtocolBody::Rejected {
        update_id: update_id.into(),
        failure: Payload::new(b"rejected".to_vec()),
    }
}

fn update_admitted(update_id: &str) -> HistoryEventKind {
    HistoryEventKind::WorkflowExecutionUpdateAdmitted {
        update_id: update_id.into(),
        update_name: "handler".into(),
        input: payloads("update"),
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

fn update_refusal(result: Result<Transition, Reject>) -> Option<UpdateLimit> {
    match result {
        Err(Reject::UpdateLimitExceeded(UpdateLimitExceeded { limit, .. })) => Some(limit),
        _ => None,
    }
}

fn ids(prefix: &'static str) -> impl Strategy<Value = BTreeSet<String>> {
    proptest::collection::btree_set(
        (0u8..6).prop_map(move |index| format!("{prefix}-{index}")),
        0..4,
    )
}

fn task_type() -> impl Strategy<Value = WorkflowTaskType> {
    prop_oneof![
        Just(WorkflowTaskType::Normal),
        Just(WorkflowTaskType::Speculative)
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: admitted-updates-after-restart, Property 1: The run keeps only the admitted updates it can deliver
    #[test]
    fn the_run_keeps_only_the_admitted_updates_it_can_deliver(
        held in ids("held"),
        lost in ids("lost"),
        history_admitted in ids("reapplied"),
        task in proptest::option::of((task_type(), any::<bool>())),
    ) {
        let mut state = with_updates(open_state(), &held, &history_admitted, 0);
        state.admitted_updates.extend(lost.iter().cloned());
        if let Some((task_type, started)) = task {
            state = with_task(state, task_type, started);
        }
        forget_lost_updates(&mut state, |update_id| held.contains(update_id));

        let mut deliverable = held.clone();
        deliverable.extend(history_admitted.iter().cloned());
        prop_assert_eq!(
            state.admitted_updates.iter().cloned().collect::<BTreeSet<_>>(),
            deliverable.clone()
        );
        prop_assert_eq!(&state.history_admitted_updates, &history_admitted);
        let dropped = task == Some((WorkflowTaskType::Speculative, false)) && held.is_empty();
        prop_assert_eq!(state.pending_workflow_task.is_some(), task.is_some() && !dropped);

        // The next command's transition carries the forget, and records nothing
        // for it: a new update's admission records no event either.
        let transition = BasicKernel
            .apply(LoadedRun::Existing(state), update_command("next", UNLIMITED, false))
            .unwrap();
        deliverable.insert("next".into());
        prop_assert_eq!(
            transition.next_state.admitted_updates.iter().cloned().collect::<BTreeSet<_>>(),
            deliverable
        );
        prop_assert_eq!(&transition.next_state.history_admitted_updates, &history_admitted);
        prop_assert!(transition.history_events.is_empty());
    }

    // Feature: admitted-updates-after-restart, Property 3: A follow-up task carries something
    #[test]
    fn a_follow_up_task_carries_something(
        held in ids("held"),
        history_admitted in ids("reapplied"),
        delivered_count in 0usize..4,
        task_type in task_type(),
    ) {
        let delivered = held.iter().take(delivered_count).cloned().collect::<Vec<_>>();
        let state = with_task(
            with_updates(open_state(), &held, &history_admitted, 0),
            task_type,
            true,
        );
        let finished = BasicKernel
            .apply(
                LoadedRun::Existing(state.clone()),
                completion(&state, Vec::new(), delivered.clone(), 0),
            )
            .unwrap()
            .next_state;
        // Only a held update the completed task didn't deliver needs a message.
        let undelivered = held.len() > delivered.len();
        prop_assert_eq!(
            finished
                .pending_workflow_task
                .as_ref()
                .is_some_and(|task| task.task_type == WorkflowTaskType::Speculative),
            undelivered
        );
        prop_assert_eq!(finished.pending_workflow_task.is_some(), undelivered);
        prop_assert_eq!(&finished.history_admitted_updates, &history_admitted);
    }

    // Feature: admitted-updates-after-restart, Property 4: In flight is held, history-admitted and accepted
    #[test]
    fn in_flight_is_held_history_admitted_and_accepted(
        held in ids("held"),
        history_admitted in ids("reapplied"),
        accepted_updates in 0usize..4,
        offset in -1i64..2,
    ) {
        let in_flight = held.len() + history_admitted.len() + accepted_updates;
        let state = with_updates(open_state(), &held, &history_admitted, accepted_updates);

        // The update limits: refused for in flight exactly at the limit.
        let limit = usize::try_from((in_flight as i64 + offset).max(1)).unwrap();
        let admission = BasicKernel.apply(
            LoadedRun::Existing(state.clone()),
            update_command("new", UpdateLimits { in_flight: limit, ..UNLIMITED }, false),
        );
        if in_flight >= limit {
            prop_assert_eq!(update_refusal(admission), Some(UpdateLimit::InFlight));
        } else {
            prop_assert!(admission.is_ok());
        }

        // A worker's re-admission: the total counts the same updates.
        let total = in_flight + 1;
        let started = with_task(state.clone(), WorkflowTaskType::Normal, true);
        let readmission = BasicKernel.apply(
            LoadedRun::Existing(started.clone()),
            completion(&started, vec![message(0, accepted("ghost"))], Vec::new(), total),
        );
        prop_assert!(readmission.is_ok(), "{readmission:?}");
        // A total of zero is no limit, so the run must hold something to be
        // at one.
        if in_flight > 0 {
            let at_total = BasicKernel.apply(
                LoadedRun::Existing(started.clone()),
                completion(&started, vec![message(0, accepted("ghost"))], Vec::new(), in_flight),
            );
            prop_assert_eq!(update_refusal(at_total), Some(UpdateLimit::Total));
        }

        // The continue-as-new advice, at its threshold and one short of it.
        let threshold = ContinueAsNewAdvicePolicy::V1_31_0.total_updates_suggest_threshold;
        for (short, suggested) in [(0, true), (1, false)] {
            let mut state = with_task(state.clone(), WorkflowTaskType::Normal, false);
            state.completed_update_count = threshold - u32::try_from(in_flight).unwrap() - short;
            let advice = start_task(&state, 0)
                .next_state
                .pending_workflow_task
                .unwrap()
                .advice;
            prop_assert_eq!(advice.suggest_continue_as_new, suggested);
        }
    }

    // Feature: admitted-updates-after-restart, Property 6: History admission follows the run
    #[test]
    fn history_admission_follows_the_run(
        reapplied in ids("reapplied"),
        answers in proptest::collection::vec(0u8..3, 6),
        closed in any::<bool>(),
    ) {
        let start = BasicKernel
            .apply(LoadedRun::Absent, Command::Start(start_request()))
            .unwrap();
        let mut history = start.history_events.to_vec();
        let mut state = start.next_state;
        let started = start_task(&state, 0);
        history.extend(started.history_events.iter().cloned());
        state = started.next_state;

        // The reset fails the started task and reapplies the updates.
        let pending = state.pending_workflow_task.clone().unwrap();
        let reset = BasicKernel
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
                    reset_reapply: reapplied.iter().map(|id| update_admitted(id)).collect(),
                    history_size_bytes: 0,
                    advice_policy: ContinueAsNewAdvicePolicy::V1_31_0,
                    terminate_reason: None,
                }),
            )
            .unwrap();
        history.extend(reset.history_events.iter().cloned());
        state = reset.next_state;
        prop_assert_eq!(&state.history_admitted_updates, &reapplied);
        prop_assert_eq!(&replay(&state, &history).history_admitted_updates, &reapplied);

        // The worker accepts some, rejects some and leaves the rest, which
        // reach it in history: nothing was delivered as a message.
        let started = start_task(&state, 1);
        history.extend(started.history_events.iter().cloned());
        state = started.next_state;
        let mut remaining = reapplied.clone();
        let mut accepted_ids = BTreeSet::new();
        let mut commands = Vec::new();
        for (index, update_id) in reapplied.iter().enumerate() {
            match answers[index] {
                0 => {
                    commands.push(message(index, accepted(update_id)));
                    accepted_ids.insert(update_id.clone());
                }
                1 => commands.push(message(index, rejected(update_id))),
                _ => continue,
            }
            remaining.remove(update_id);
        }
        let answered = BasicKernel
            .apply(
                LoadedRun::Existing(state.clone()),
                completion(&state, commands, Vec::new(), 0),
            )
            .unwrap();
        history.extend(answered.history_events.iter().cloned());
        state = answered.next_state;
        prop_assert_eq!(&state.history_admitted_updates, &remaining);
        // A rejection writes no event, so the replay of the copied history
        // admits a rejected update again, as v1.31.0's rejection leaves
        // mutable state as it was (`RejectWorkflowExecutionUpdate`,
        // `mutable_state_impl.go:5388-5391 @ v1.31.0`).
        let mut replayed = reapplied.clone();
        replayed.retain(|update_id| !accepted_ids.contains(update_id));
        prop_assert_eq!(&replay(&state, &history).history_admitted_updates, &replayed);

        if closed {
            let terminated = BasicKernel
                .apply(
                    LoadedRun::Existing(state),
                    Command::Terminate(TerminateRequest {
                        reason: "done".into(),
                        details: None,
                        identity: "tester".into(),
                        links: Vec::new(),
                        request: request("terminate"),
                        now: now(),
                    }),
                )
                .unwrap();
            prop_assert!(terminated.next_state.history_admitted_updates.is_empty());
        }
    }
}

/// A speculative workflow task a worker has started stays for its completion,
/// though the forget leaves it nothing to deliver.
#[test]
fn the_forget_keeps_a_started_speculative_task() {
    let lost = BTreeSet::from(["lost-0".to_string()]);
    let mut state = with_task(
        with_updates(open_state(), &lost, &BTreeSet::new(), 0),
        WorkflowTaskType::Speculative,
        true,
    );
    let task = state.pending_workflow_task.clone();
    forget_lost_updates(&mut state, |_| false);
    assert!(state.admitted_updates.is_empty());
    assert_eq!(state.pending_workflow_task, task);
}

/// A lost update's retry, marked `readmit`, is admitted anew and delivered by a
/// fresh speculative task; without the mark, its id is a duplicate. An update
/// its event delivers is never taken over (`updateworkflow/api.go:161-188 @
/// v1.31.0`).
#[test]
fn a_readmitted_update_is_admitted_anew() {
    let retried = BTreeSet::from(["retried".to_string()]);
    let state = with_updates(open_state(), &retried, &BTreeSet::new(), 0);
    assert_eq!(
        BasicKernel
            .apply(
                LoadedRun::Existing(state.clone()),
                update_command("retried", UNLIMITED, false)
            )
            .unwrap_err(),
        Reject::DuplicateUpdateId("retried".into())
    );
    // With the in-flight limit at one, the retry still fits: it is the one
    // being admitted, not another.
    let limits = UpdateLimits {
        in_flight: 1,
        ..UNLIMITED
    };
    let readmitted = BasicKernel
        .apply(
            LoadedRun::Existing(state),
            update_command("retried", limits, true),
        )
        .unwrap()
        .next_state;
    assert!(readmitted.admitted_updates.contains("retried"));
    assert!(
        readmitted
            .pending_workflow_task
            .is_some_and(|task| task.task_type == WorkflowTaskType::Speculative)
    );

    let reapplied = BTreeSet::from(["reapplied".to_string()]);
    let state = with_updates(open_state(), &BTreeSet::new(), &reapplied, 0);
    assert_eq!(
        BasicKernel
            .apply(
                LoadedRun::Existing(state),
                update_command("reapplied", UNLIMITED, true)
            )
            .unwrap_err(),
        Reject::DuplicateUpdateId("reapplied".into())
    );
}
