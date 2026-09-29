//! Properties of resolved execution lineage and update-with-start retries.

use std::{cell::Cell, collections::BTreeSet, future::ready};

use proptest::prelude::*;
use tokeira_kernel::{
    BasicKernel, Kernel, ReplayContext, StartWorkflowTaskRequest, UpdateRequest, WorkflowCommand,
    WorkflowState, WorkflowTaskCompletedRequest, WorkflowTaskFailedCause,
    WorkflowTaskFailedRequest,
};
use tokeira_storage::InMemoryStore;
use tokeira_types::{RequestId, ShardEpoch, WorkflowTaskToken};

use super::{
    tests::{deletion_runtime, deletion_start_request},
    *,
};

fn signal_start(start: &StartRequest) -> SignalWithStartRequest {
    let mut value = serde_json::to_value(start).unwrap();
    value["signal_name"] = "signal".into();
    value["signal_input"] = serde_json::to_value(Payloads::default()).unwrap();
    serde_json::from_value(value).unwrap()
}

#[tokio::test]
async fn only_one_concurrent_admission_can_consume_the_last_slot() {
    let repo = InMemoryStore::default();
    let start = deletion_start_request();
    let state = commit_command(
        &repo,
        LoadedRun::Absent,
        Command::Start(start.clone()),
        false,
    )
    .await;
    let mut transitions = Vec::new();
    let mut requests = Vec::new();
    for id in ["one", "two"] {
        let command = Command::Update(UpdateRequest {
            update_id: id.into(),
            update_name: "handler".into(),
            input: Payloads::default(),
            request: RequestContext {
                request_id: RequestId(id.into()),
                ..RequestContext::unattributed(start.now)
            },
            now: start.now,
        });
        let loaded = LoadedRun::Existing(state.clone());
        assert!(
            !crate::update_admission::check_admission(&repo, &loaded, &command, 1)
                .await
                .unwrap()
        );
        transitions.push(BasicKernel.apply(loaded, command.clone()).unwrap());
        requests.push(command);
    }
    let winner = repo
        .commit_transition(start.run_key, transitions.remove(0), ShardEpoch::ZERO)
        .await
        .unwrap();
    assert!(matches!(winner, CommitResult::Applied { .. }));
    let loser = repo
        .commit_transition(start.run_key, transitions.remove(0), ShardEpoch::ZERO)
        .await
        .unwrap();
    assert!(matches!(loser, CommitResult::Conflict { .. }));
    let reloaded = repo.load_run(start.run_key).await.unwrap();
    let error = crate::update_admission::check_admission(&repo, &reloaded, &requests[1], 1)
        .await
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<crate::UpdateLimitExceeded>(),
        Some(&crate::UpdateLimitExceeded { limit: 1 })
    );
}

async fn commit_command(
    repo: &InMemoryStore,
    loaded: LoadedRun,
    command: Command,
    backlinks: bool,
) -> WorkflowState {
    let mut transition = BasicKernel.apply(loaded.clone(), command.clone()).unwrap();
    crate::signal_backlinks::record_with_mode(repo, &loaded, &command, &mut transition, backlinks)
        .await
        .unwrap();
    let result = repo
        .commit_transition(transition.next_state.run_key, transition, ShardEpoch::ZERO)
        .await
        .unwrap();
    let CommitResult::Applied { new_state } = result else {
        panic!("fenced commit: {result:?}")
    };
    new_state
}

async fn complete_commands(
    repo: &InMemoryStore,
    mut state: WorkflowState,
    commands: Vec<WorkflowCommand>,
    backlinks: bool,
) -> WorkflowState {
    let pending = state.pending_workflow_task.as_ref().unwrap();
    if pending.started_event_id.is_none() {
        let request = StartWorkflowTaskRequest {
            logical_seq: pending.logical_seq,
            worker_identity: WorkerIdentity("worker".into()),
            request_id: format!("start-task-{}", pending.logical_seq.0),
            history_size_bytes: 0,
            advice_policy: tokeira_kernel::ContinueAsNewAdvicePolicy::V1_31_0,
            deployment_transition: None,
            deployment_transition_revision_number: None,
            polled_task_queue: state.task_queue.clone(),
            now: state.started_at,
            target_version_changed_enabled: false,
            target_deployment_version: None,
        };
        state = commit_command(
            repo,
            LoadedRun::Existing(state),
            Command::WorkflowTaskStarted(request),
            backlinks,
        )
        .await;
    }
    let pending = state.pending_workflow_task.as_ref().unwrap();
    let request = WorkflowTaskCompletedRequest {
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
        force_new_workflow_task: true,
        limits: Default::default(),
        delivered_update_ids: Vec::new(),
        request: RequestContext::unattributed(state.started_at),
        now: state.started_at,
    };
    commit_command(
        repo,
        LoadedRun::Existing(state),
        Command::WorkflowTaskCompleted(request),
        backlinks,
    )
    .await
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    // Feature: v132-lifecycle-fidelity, Property 3: total-updates limit at admission
    // Count distinct admitted/completed IDs across durable reloads; retries do not
    // consume capacity (service/history/workflow/update/registry.go:438-449, 455-491 @ v1.32.0).
    #[test]
    fn update_sequences_obey_distinct_id_budget(limit in 0i64..8, requests in prop::collection::vec((0u8..12, any::<bool>()), 1..35)) {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let repo = InMemoryStore::default();
            let start = deletion_start_request();
            let _started = commit_command(&repo, LoadedRun::Absent, Command::Start(start.clone()), false).await;
            let mut seen = BTreeSet::new();
            let mut completed = BTreeSet::new();
            for (index, (id, complete)) in requests.into_iter().enumerate() {
                let id = format!("update-{id}");
                let command = Command::Update(UpdateRequest {
                    update_id: id.clone(), update_name: "handler".into(), input: Payloads::default(),
                    request: RequestContext { request_id: RequestId(format!("request-{index}")), ..RequestContext::unattributed(start.now) }, now: start.now,
                });
                let loaded = repo.load_run(start.run_key).await.unwrap();
                let result = crate::update_admission::check_admission(&repo, &loaded, &command, limit).await;
                let rejected = !seen.contains(&id) && limit != 0 && seen.len() >= limit as usize;
                assert_eq!(result.is_err(), rejected);
                if rejected {
                    assert!(result.unwrap_err().is::<crate::UpdateLimitExceeded>());
                    continue;
                }
                assert_eq!(result.unwrap(), completed.contains(&id));
                if completed.contains(&id) { continue; }
                let mut state = if seen.contains(&id) {
                    assert!(matches!(BasicKernel.apply(loaded.clone(), command), Err(tokeira_kernel::Reject::DuplicateUpdateId(_))));
                    let LoadedRun::Existing(state) = loaded else { panic!("existing admitted run") };
                    state
                } else {
                    commit_command(&repo, loaded, command, false).await
                };
                seen.insert(id.clone());
                if complete {
                    state = complete_commands(&repo, state, vec![
                        WorkflowCommand::ProtocolMessage { message_id: format!("accept-{id}"), body: tokeira_kernel::UpdateProtocolBody::Accepted {
                            update_id: id.clone(), update_name: "handler".into(), input: Payloads::default(), sequencing_event_id: 1,
                        } },
                        WorkflowCommand::UpdateCompleted { update_id: id.clone(), result: Payloads::default() },
                    ], false).await;
                    completed.insert(id);
                }
                let encoded = serde_json::to_vec(&state).unwrap();
                let decoded: WorkflowState = serde_json::from_slice(&encoded).unwrap();
                assert_eq!(decoded, state);
            }
        });
    }

    // Feature: v132-lifecycle-fidelity, Property 9: signal request-id infos follow the override
    // Buffered entries become real event references and are rebuilt from replayed
    // reset history (service/history/workflow/mutable_state_impl.go:6156-6186 @ v1.32.0).
    #[test]
    fn signal_backlinks_survive_buffer_flush_and_reset_replay(enabled in any::<bool>(), count in 1usize..7) {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let repo = InMemoryStore::default();
            let mut start = deletion_start_request();
            start.reserved_poller_identity = Some(WorkerIdentity("worker".into()));
            let mut state = commit_command(&repo, LoadedRun::Absent, Command::Start(start.clone()), enabled).await;
            for index in 0..count {
                let id = format!("signal-{index}");
                let signal = SignalRequest { signal_name: "signal".into(), input: Payloads::default(), header: None, links: Vec::new(),
                    request: RequestContext { request_id: RequestId(id.clone()), ..RequestContext::unattributed(start.now) }, now: start.now };
                state = commit_command(&repo, LoadedRun::Existing(state), Command::Signal(signal), enabled).await;
                assert_eq!(state.request_id_infos.contains_key(&id), enabled);
                if enabled {
                    let entry = &state.request_id_infos[&id];
                    assert!(entry.buffered);
                    assert_eq!(entry.event_type, tokeira_proto::enums::EventType::WorkflowExecutionSignaled as i32);
                }
            }
            state = complete_commands(&repo, state, Vec::new(), enabled).await;
            let history = repo.read_history(start.run_key, 0, 100).await.unwrap();
            for event in &history {
                if let HistoryEventKind::WorkflowExecutionSignaled { request_id, .. } = &event.kind {
                    assert_eq!(state.request_id_infos.contains_key(request_id), enabled);
                    if enabled {
                        assert_eq!(state.request_id_infos[request_id].event_id, event.event_id);
                        assert!(!state.request_id_infos[request_id].buffered);
                    }
                }
            }
            let mut replayed = BasicKernel.replay_history_prefix(ReplayContext {
                run_key: state.run_key, namespace_id: state.namespace_id, workflow_id: state.workflow_id.clone(), run_id: state.run_id,
                deployment: None, build_id: None, parent_run_key: None, parent_workflow_id: None, first_run_started_at: None,
            }, &history).unwrap();
            replayed.transition_seq = state.transition_seq;
            let pending = replayed.pending_workflow_task.as_ref().unwrap();
            let reset = WorkflowTaskFailedRequest {
                logical_seq: pending.logical_seq, started_event_id: pending.started_event_id.unwrap_or(0),
                failure_cause: WorkflowTaskFailedCause::ResetWorkflow, failure_details: None,
                worker_identity: WorkerIdentity("history-service".into()), request: RequestContext::unattributed(start.now), now: start.now,
                reset_reapply: Vec::new(), history_size_bytes: 0, advice_policy: tokeira_kernel::ContinueAsNewAdvicePolicy::V1_31_0,
            };
            let rebuilt = commit_command(&repo, LoadedRun::Existing(replayed), Command::WorkflowTaskFailed(reset), enabled).await;
            for index in 0..count {
                let id = format!("signal-{index}");
                assert_eq!(rebuilt.request_id_infos.get(&id), state.request_id_infos.get(&id));
            }
        });
    }

    // Feature: v132-lifecycle-fidelity, Property 1: chain head propagates to every start-path outcome
    // The oracle is event 1 of an independently resolved run, including inherited
    // and reset-shaped starts (service/history/api/startworkflow/api.go:333-359, 736-762 @ v1.32.0).
    #[test]
    fn resolved_lineage_reaches_all_start_outcomes(chain in any::<u128>(), kind in 0u8..5, start_id in "[a-z]{1,20}") {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let repo = Arc::new(InMemoryStore::default());
            let runtime = deletion_runtime(repo.clone());
            let mut start = deletion_start_request();
            start.request.request_id = RequestId(start_id.clone());
            start.now = OffsetDateTime::now_utc();
            if kind != 0 {
                start.first_execution_run_id = Some(RunId(uuid::Uuid::from_u128(chain)));
                start.continued_execution_run_id = Some(RunId::new());
                start.initiator = match kind {
                    1 => Some(tokeira_kernel::ContinueAsNewInitiator::Retry),
                    2 => Some(tokeira_kernel::ContinueAsNewInitiator::CronSchedule),
                    3 => Some(tokeira_kernel::ContinueAsNewInitiator::Workflow),
                    _ => None,
                };
                if kind == 4 { start.original_execution_run_id = Some(RunId::new()); }
            }
            let created = runtime.start_workflow_with_policy(start.clone()).await.unwrap();
            let history = repo.read_history(start.run_key, 0, 1).await.unwrap();
            let expected = match history[0].kind {
                HistoryEventKind::WorkflowExecutionStartedV2 { first_execution_run_id, .. } => first_execution_run_id.unwrap_or(start.run_id),
                _ => panic!("event 1 must be a start"),
            };
            assert!(matches!(created, StartWorkflowResult::Started { first_execution_run_id, .. } if first_execution_run_id == expected));
            let dedup = runtime.start_workflow_with_policy(start.clone()).await.unwrap();
            assert!(matches!(dedup, StartWorkflowResult::Deduped { first_execution_run_id, .. } if first_execution_run_id == expected));
            let mut another = start.clone();
            another.run_key = RunKey::new();
            another.run_id = RunId::new();
            another.first_execution_run_id = None;
            another.request.request_id = RequestId(format!("another-{start_id}"));
            let rejected = runtime.start_workflow_with_policy(another.clone()).await.unwrap();
            assert!(matches!(rejected, StartWorkflowResult::Rejected { run_id, first_execution_run_id, start_request_id, .. }
                if run_id == start.run_id && first_execution_run_id == expected && start_request_id == start_id));
            another.conflict_policy = WorkflowIdConflictPolicy::UseExisting;
            let used = runtime.start_workflow_with_policy(another.clone()).await.unwrap();
            assert!(matches!(used, StartWorkflowResult::UsedExisting { first_execution_run_id, .. } if first_execution_run_id == expected));
            let signaled = runtime.signal_with_start_workflow(signal_start(&another)).await.unwrap();
            assert!(matches!(signaled, SignalWithStartResult::Signaled { first_execution_run_id, .. } if first_execution_run_id == expected));
            let multi = runtime.execute_multi_operation(another.clone(), "update".into(), "handler".into(), Payloads::default(),
                RequestContext::unattributed(start.now), Duration::seconds(1), UpdateWaitPolicy::Admitted).await.unwrap();
            assert_eq!(multi.run_id, start.run_id);
            assert_eq!(multi.first_execution_run_id, expected);
            assert!(!multi.started);
            assert_eq!(multi.execution_status, ExecutionStatus::Running);
        });
    }

    // Feature: v132-lifecycle-fidelity, Property 4: update-with-start re-executes once on a closing abort
    // Only an update-only closing abort is retried, once; started and unrelated
    // failures remain single attempts (service/history/api/multioperation/api.go:126-180 @ v1.32.0).
    #[test]
    fn update_with_start_retry_is_bounded(first in 0u8..4, second in 0u8..4) {
        let calls = Cell::new(0);
        let result = tokio::runtime::Runtime::new().unwrap().block_on(retry_closing_update(|| {
            let outcome = if calls.get() == 0 { first } else { second };
            calls.set(calls.get() + 1);
            ready(match outcome {
                0 => Ok(42),
                1 | 2 => Err(MultiOperationError::UpdateFailed {
                    started: outcome == 2, source: crate::UpdateAbortedByClosingWorkflow.into(),
                }.into()),
                _ => Err(anyhow!("unrelated error")),
            })
        }));
        prop_assert_eq!(calls.get(), if first == 1 { 2 } else { 1 });
        if first == 1 && second == 1 {
            let error = result.unwrap_err();
            let Some(MultiOperationError::UpdateFailed { started, source }) = error.downcast_ref() else { panic!("typed failure") };
            prop_assert!(!started);
            prop_assert!(source.is::<crate::UpdateWithStartRetryExhausted>());
        } else {
            prop_assert_eq!(result.is_ok(), if first == 1 { second == 0 } else { first == 0 });
        }
    }

    // Feature: v132-lifecycle-fidelity, Property 7: signal links are unconditional and idempotent
    // Retrying a signal-with-start on the created run must not add another event
    // (service/history/api/signalwithstartworkflow/api.go:96-104; service/history/api/signalworkflow/api.go:93-122 @ v1.32.0).
    #[test]
    fn signal_with_start_retry_preserves_history(id in "[a-z]{1,32}") {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let repo = Arc::new(InMemoryStore::default());
            let runtime = deletion_runtime(repo.clone());
            let mut request = signal_start(&deletion_start_request());
            request.request.request_id = RequestId(id);
            request.conflict_policy = WorkflowIdConflictPolicy::UseExisting;
            runtime.signal_with_start_workflow(request.clone()).await.unwrap();
            let before = repo.read_history(request.run_key, 0, 100).await.unwrap();
            runtime.signal_with_start_workflow(request.clone()).await.unwrap();
            let after = repo.read_history(request.run_key, 0, 100).await.unwrap();
            assert_eq!(before, after);
        });
    }
}
