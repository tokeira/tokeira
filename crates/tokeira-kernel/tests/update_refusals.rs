//! `update-refusals` in the kernel. A run keeps the id of each update that has
//! completed in it, as its history records, and its replay rebuilds the same
//! ids. An acceptance, a rejection or a response for a completed update fails
//! the workflow task with v1.31.0's invalid state transition, and writes
//! nothing. An update request to a run whose task keeps failing is refused.
// Integration test: unwrap is idiomatic in test code (root AGENTS.md §1).
#![allow(clippy::unwrap_used)]

use std::collections::BTreeSet;

use proptest::prelude::*;
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    BasicKernel, Command, ContinueAsNewAdvicePolicy, HistoryEvent, LoadedRun, Reject,
    ReplayContext, SignalRequest, StartRequest, StartWorkflowTaskRequest, Transition,
    UpdateProtocolBody, UpdateRequest, WorkflowCommand, WorkflowState,
    WorkflowTaskCompletedRequest, WorkflowTaskCompletionLimits, completed_update_ids,
    event::{HistoryEventKind, UpdateEventOutcome},
    kernel::Kernel,
    limits::UpdateLimits,
};
use tokeira_types::{
    Memo, NamespaceId, Payload, Payloads, RequestContext, RequestId, RunId, RunKey,
    SearchAttributes, ShardEpoch, TaskQueueName, WorkerIdentity, WorkflowId, WorkflowTaskToken,
    WorkflowType,
};

/// v1.31.0's default total update limit (`history.maxTotalUpdates`).
const TOTAL_UPDATES: usize = 2_000;

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

fn start_request() -> StartRequest {
    let run_id = RunId::new();
    let namespace_id = NamespaceId::new();
    let workflow_id = WorkflowId("refused-workflow".into());
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

/// A run in progress, with every event it has written.
struct Run {
    state: WorkflowState,
    history: Vec<HistoryEvent>,
    commands: usize,
}

impl Run {
    fn start() -> Self {
        let start = BasicKernel
            .apply(LoadedRun::Absent, Command::Start(start_request()))
            .unwrap();
        let mut run = Self {
            state: start.next_state.clone(),
            history: Vec::new(),
            commands: 0,
        };
        run.record(start);
        run
    }

    fn record(&mut self, transition: Transition) {
        self.history
            .extend(transition.history_events.iter().cloned());
        self.state = transition.next_state;
    }

    fn apply(&mut self, command: Command) -> Result<(), Reject> {
        let transition = BasicKernel.apply(LoadedRun::Existing(self.state.clone()), command)?;
        self.record(transition);
        Ok(())
    }

    fn admit(&mut self, update_id: &str) -> Result<(), Reject> {
        self.commands += 1;
        self.apply(Command::Update(UpdateRequest {
            update_id: update_id.into(),
            update_name: "handler".into(),
            input: payloads("update"),
            request: request(&format!("update-{}", self.commands)),
            now: now(),
            limits: UpdateLimits {
                in_flight: 0,
                in_flight_payloads: 0,
                total: 0,
            },
            request_bytes: 0,
            in_flight_request_bytes: 0,
            readmit: false,
        }))
    }

    /// Give the run a started workflow task: signal it when it has none, then
    /// start the pending one when it isn't started.
    fn started_task(&mut self) {
        self.commands += 1;
        if self.state.pending_workflow_task.is_none() {
            self.apply(Command::Signal(SignalRequest {
                signal_name: "wake".into(),
                input: Payloads::default(),
                header: None,
                links: Vec::new(),
                request: request(&format!("signal-{}", self.commands)),
                now: now(),
            }))
            .unwrap();
        }
        let pending = self.state.pending_workflow_task.as_ref().unwrap();
        if pending.started_event_id.is_none() {
            let logical_seq = pending.logical_seq;
            self.apply(Command::WorkflowTaskStarted(StartWorkflowTaskRequest {
                advice_policy: ContinueAsNewAdvicePolicy::V1_31_0,
                logical_seq,
                worker_identity: WorkerIdentity("worker".into()),
                request_id: format!("start-task-{}", self.commands),
                history_size_bytes: 0,
                deployment_transition: None,
                deployment_transition_revision_number: None,
                target_version_changed_enabled: false,
                target_deployment_version: None,
                polled_task_queue: TaskQueueName("queue".into()),
                now: now(),
            }))
            .unwrap();
        }
    }

    /// Complete the run's workflow task with `bodies` as protocol messages.
    fn complete(&mut self, bodies: Vec<UpdateProtocolBody>) -> Result<(), Reject> {
        self.started_task();
        let pending = self.state.pending_workflow_task.as_ref().unwrap();
        let delivered = self.state.admitted_updates.iter().cloned().collect();
        let commands = bodies
            .into_iter()
            .enumerate()
            .map(|(index, body)| WorkflowCommand::ProtocolMessage {
                message_id: format!("message-{}-{index}", self.commands),
                body,
            })
            .collect();
        self.apply(Command::WorkflowTaskCompleted(
            WorkflowTaskCompletedRequest {
                token: WorkflowTaskToken {
                    run_key: self.state.run_key,
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
                    total_updates: TOTAL_UPDATES,
                    ..WorkflowTaskCompletionLimits::default()
                },
                delivered_update_ids: delivered,
                request: RequestContext::unattributed(OffsetDateTime::UNIX_EPOCH),
                now: now(),
            },
        ))
    }

    /// The state a replay of the run's history rebuilds, as a reset's new run
    /// rebuilds the history it copies.
    fn replayed(&self) -> WorkflowState {
        BasicKernel
            .replay_history_prefix(
                ReplayContext {
                    run_key: self.state.run_key,
                    namespace_id: self.state.namespace_id,
                    workflow_id: self.state.workflow_id.clone(),
                    run_id: self.state.run_id,
                    deployment: None,
                    build_id: None,
                    parent_run_key: None,
                    parent_workflow_id: None,
                    first_run_started_at: self.state.first_run_started_at,
                },
                &self.history,
            )
            .unwrap()
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

fn responded(update_id: &str) -> UpdateProtocolBody {
    UpdateProtocolBody::Completed {
        update_id: update_id.into(),
        result: payloads("result"),
        failure: None,
    }
}

fn rejected(update_id: &str) -> UpdateProtocolBody {
    UpdateProtocolBody::Rejected {
        update_id: update_id.into(),
        failure: Payload::new(b"rejected".to_vec()),
    }
}

/// v1.31.0's answer to `message_type` for a completed update.
fn completed_refusal(update_id: &str, message_type: &str) -> Reject {
    Reject::BadUpdateMessage {
        message: format!(
            "invalid state transition attempted for Update {update_id}: received \
             *update.{message_type} message while in state Completed"
        ),
        not_found: false,
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    // Feature: update-refusals, Property 3: The completed ids are the history's
    /// Whatever the run's admissions, acceptances, responses and rejections, its
    /// completed ids are those of the completion events in its history, after
    /// every step, and the state a replay of that history rebuilds, as a reset's
    /// new run does, holds the same ids.
    #[test]
    fn the_completed_ids_are_the_historys(
        ops in prop::collection::vec((0u8..5, 0u8..3), 1..16),
    ) {
        let mut run = Run::start();
        for (kind, slot) in ops {
            let update_id = format!("update-{slot}");
            let in_flight = run.state.admitted_updates.contains(&update_id)
                || run.state.pending_updates.contains_key(&update_id);
            // A refused step changes nothing, which the checks below still hold for.
            let _ = match kind {
                0 if !in_flight => run.admit(&update_id),
                1 if !run.state.pending_updates.contains_key(&update_id) => {
                    run.complete(vec![accepted(&update_id)])
                }
                2 if run.state.pending_updates.contains_key(&update_id) => {
                    run.complete(vec![responded(&update_id)])
                }
                3 => run.complete(vec![rejected(&update_id)]),
                // An admitted update accepted and completed in one task.
                4 if run.state.admitted_updates.contains(&update_id) => {
                    run.complete(vec![accepted(&update_id), responded(&update_id)])
                }
                _ => Ok(()),
            };
            prop_assert_eq!(&run.state.completed_updates, &completed_update_ids(&run.history));
            prop_assert_eq!(&run.replayed().completed_updates, &run.state.completed_updates);
        }
    }

    // Feature: update-refusals, Property 4: A message for a completed update fails the task
    /// An acceptance, a rejection or a response for any of a run's completed
    /// updates fails the workflow task with v1.31.0's invalid state transition,
    /// so the run writes no event for it and keeps its counts.
    #[test]
    fn a_message_for_a_completed_update_fails_the_task(
        completed in 1usize..4,
        target in 0usize..4,
        message_type in 0u8..3,
    ) {
        let mut run = Run::start();
        for index in 0..completed {
            let update_id = format!("update-{index}");
            run.admit(&update_id).unwrap();
            run.complete(vec![accepted(&update_id), responded(&update_id)]).unwrap();
        }
        let update_id = format!("update-{}", target % completed);
        run.started_task();
        let before = run.state.clone();
        let (body, name) = match message_type {
            0 => (accepted(&update_id), "Acceptance"),
            1 => (rejected(&update_id), "Rejection"),
            _ => (responded(&update_id), "Response"),
        };
        let refused = run.complete(vec![body]);
        prop_assert_eq!(refused, Err(completed_refusal(&update_id, name)));
        prop_assert_eq!(&run.state, &before);
        prop_assert_eq!(run.state.completed_update_count as usize, completed);
    }
}

/// The ids of the completion events in either form, and of no other event.
#[test]
fn completed_update_ids_reads_both_completion_forms() {
    let event = |event_id, kind| HistoryEvent {
        event_id,
        kind,
        ..Run::start().history[0].clone()
    };
    let history = vec![
        event(
            1,
            HistoryEventKind::WorkflowExecutionUpdateCompleted {
                update_id: "legacy".into(),
                accepted_event_id: 1,
                result: payloads("result"),
            },
        ),
        event(
            2,
            HistoryEventKind::WorkflowExecutionUpdateCompletedV2 {
                update_id: "current".into(),
                update_name: "handler".into(),
                accepted_event_id: 1,
                outcome: UpdateEventOutcome::Success(payloads("result")),
            },
        ),
        event(
            3,
            HistoryEventKind::WorkflowExecutionUpdateAccepted {
                update_id: "accepted-only".into(),
                update_name: "handler".into(),
                input: payloads("update"),
                accepted_request_sequencing_event_id: 1,
            },
        ),
    ];
    assert_eq!(
        completed_update_ids(&history),
        BTreeSet::from(["current".to_string(), "legacy".to_string()])
    );
}

/// A run whose workflow task attempt has reached 3 refuses a new update, and
/// one whose attempt is 2 takes it (`updateworkflow/api.go:132-143 @ v1.31.0`).
#[test]
fn an_update_is_refused_from_the_third_task_attempt() {
    for (attempt, refused) in [(1, false), (2, false), (3, true), (4, true)] {
        let mut run = Run::start();
        run.state.workflow_task_attempt = attempt;
        let result = run.admit("update-0");
        assert_eq!(
            matches!(result, Err(Reject::UpdateWorkflowTaskFailing)),
            refused,
            "attempt {attempt}: {result:?}"
        );
    }
}
