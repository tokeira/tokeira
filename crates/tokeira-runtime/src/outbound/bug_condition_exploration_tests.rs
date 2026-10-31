//! Crash cuts through the real lane and repository, with publication discarded.
//!
//! These assert the corrected contract and must fail on the recorded baseline.
//! A fresh runtime gets only committed storage; no captured dispatch is replayed.

use std::sync::{
    Arc, Mutex, RwLock,
    atomic::{AtomicUsize, Ordering},
};

use anyhow::{Result, bail};
use async_trait::async_trait;
use opentelemetry::KeyValue;
use proptest::prelude::*;
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    BasicKernel, ChildStartConfirmedRequest, ChildStartResult, Command, DispatchOp,
    HistoryEventKind, LoadedRun, NexusOperationResolvedRequest, NexusResolution, ParentClosePolicy,
    StartRequest, StartWorkflowTaskRequest, WorkflowCommand, WorkflowIdReusePolicy, WorkflowState,
    WorkflowTaskCompletedRequest,
};
use tokeira_storage::{CommitResult, InMemoryStore, RunRepository};
use tokeira_types::{
    ExecutionRef, Memo, Payload, Payloads, RequestContext, RunId, RunKey, SearchAttributes,
    ShardEpoch, ShardId, TaskQueueName, WorkerIdentity, WorkflowId, WorkflowTaskToken,
};

use crate::{
    LaneConfig, TokeiraRuntime, UpdateRegistry,
    activity_timeout::ActivityTrackingState,
    lane::{DispatchPublisher, LaneHandle, spawn_lane},
    nexus::{
        InMemoryNexusEndpointStore, NexusCancelResult, NexusCompletionDeps, NexusEndpointRegistry,
        NexusEndpointSpec, NexusEndpointSpecTarget, NexusEndpointStore, NexusHttpClient,
        NexusStartResult, NexusTimeoutTrackingState,
    },
    runtime::{RuntimeConfig, tests::sample_start_request},
    shard::ShardOwner,
    timeout::WorkflowTimeoutTrackingState,
    wft_timeout::WftTimeoutTrackingState,
};

use super::test_repository::Repository;

#[derive(Debug, Default)]
struct HeldCallbackClient {
    calls: Mutex<Vec<crate::nexus::NexusCompletion>>,
    release: tokio::sync::Notify,
}

#[async_trait]
impl crate::nexus::NexusCompletionClient for HeldCallbackClient {
    async fn complete_operation(
        &self,
        _: &str,
        _: &str,
        _: &str,
        completion: crate::nexus::NexusCompletion,
        _: &[tokeira_kernel::Link],
    ) -> Result<crate::nexus::CompletionDeliveryOutcome> {
        self.calls.lock().unwrap().push(completion);
        self.release.notified().await;
        Ok(crate::nexus::CompletionDeliveryOutcome::Delivered)
    }
}

#[derive(Clone, Debug)]
struct LostPublication;

#[async_trait]
impl DispatchPublisher for LostPublication {
    async fn publish(&self, _: RunKey, _: ShardId, _: &[DispatchOp]) -> Result<()> {
        Ok(())
    }

    async fn submit_to_run(&self, _: RunKey, _: Command) -> Result<CommitResult> {
        bail!("crash cut: follow-up submission lost")
    }
}

struct Fixture {
    repo: Arc<Repository>,
    lane: LaneHandle,
    source: StartRequest,
    target: StartRequest,
}

#[derive(Debug, Default)]
struct NexusClient {
    starts: AtomicUsize,
    cancels: AtomicUsize,
}

#[async_trait]
impl NexusHttpClient for NexusClient {
    async fn start_operation(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &str,
        _: &Payloads,
        _: Option<Duration>,
        _: &[KeyValue],
    ) -> Result<NexusStartResult> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        Ok(NexusStartResult::SyncCompleted {
            result: Payloads::default(),
            links: Vec::new(),
        })
    }

    async fn cancel_operation(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &str,
        _: &[KeyValue],
    ) -> Result<NexusCancelResult> {
        self.cancels.fetch_add(1, Ordering::SeqCst);
        Ok(NexusCancelResult::Succeeded)
    }
}

fn executor() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap()
}

fn request(label: &str, seed: u64) -> StartRequest {
    let mut request = sample_start_request(None, None);
    request.workflow_id = WorkflowId(format!("outbound-{label}-{seed}"));
    request.namespace_id = tokeira_types::NamespaceId(uuid::Uuid::from_u128(u128::from(seed) + 1));
    request.run_id = RunId(uuid::Uuid::from_u128(
        u128::from(seed) * 8 + if label == "source" { 1 } else { 2 },
    ));
    request.run_key = RunKey::derive(request.namespace_id, &request.workflow_id, request.run_id);
    request.task_queue = TaskQueueName(format!("outbound-{label}"));
    request.request.request_id.0 = format!("start-{label}-{seed}");
    request.now = OffsetDateTime::now_utc();
    request.request.received_at = request.now;
    request
}

async fn state(repo: &InMemoryStore, key: RunKey) -> WorkflowState {
    let LoadedRun::Existing(state) = repo.load_run(key).await.unwrap() else {
        panic!("fixture run must exist")
    };
    state
}

fn applied(result: CommitResult) -> WorkflowState {
    let CommitResult::Applied { new_state } = result else {
        panic!("fixture transition must apply: {result:?}")
    };
    new_state
}

impl Fixture {
    async fn new(seed: u64) -> Self {
        let repo = Arc::new(Repository::default());
        let mut owner = ShardOwner::new(1);
        owner.record_acquired(ShardId(0), ShardEpoch::ZERO);
        owner.mark_active(ShardId(0));
        let lane = spawn_lane(
            BasicKernel,
            repo.clone(),
            LostPublication,
            Arc::new(RwLock::new(owner)),
            ActivityTrackingState::default(),
            WorkflowTimeoutTrackingState::default(),
            WftTimeoutTrackingState::default(),
            NexusTimeoutTrackingState::default(),
            UpdateRegistry::new(),
            LaneConfig::default(),
        );
        let source = request("source", seed);
        let mut target = request("target", seed);
        target.namespace_id = source.namespace_id;
        target.run_key = RunKey::derive(target.namespace_id, &target.workflow_id, target.run_id);
        applied(
            lane.submit(source.run_key, Command::Start(source.clone()))
                .await
                .unwrap(),
        );
        Self {
            repo,
            lane,
            source,
            target,
        }
    }

    async fn complete(
        &self,
        start: &StartRequest,
        commands: Vec<WorkflowCommand>,
    ) -> WorkflowState {
        let before = state(&self.repo, start.run_key).await;
        let pending = before.pending_workflow_task.unwrap();
        let started = applied(
            self.lane
                .submit(
                    start.run_key,
                    Command::WorkflowTaskStarted(StartWorkflowTaskRequest {
                        logical_seq: pending.logical_seq,
                        worker_identity: WorkerIdentity("exploration".into()),
                        request_id: format!("wft:{}:{}", start.run_id.0, pending.logical_seq.0),
                        history_size_bytes: 0,
                        advice_policy: tokeira_kernel::ContinueAsNewAdvicePolicy::V1_31_0,
                        deployment_transition: None,
                        deployment_transition_revision_number: None,
                        polled_task_queue: start.task_queue.clone(),
                        now: start.now,
                        target_version_changed_enabled: false,
                        target_deployment_version: None,
                    }),
                )
                .await
                .unwrap(),
        );
        let task = started.pending_workflow_task.unwrap();
        let successor_plan = commands
            .iter()
            .any(|command| matches!(command, WorkflowCommand::ContinueAsNew { .. }))
            .then(|| crate::successor::SuccessorPlan::ContinueAsNew { links: Vec::new() });
        applied(
            self.lane
                .submit_with_successor(
                    start.run_key,
                    Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                        client_discards_speculative_with_events: false,
                        token: WorkflowTaskToken {
                            run_key: start.run_key,
                            logical_seq: task.logical_seq,
                            started_event_id: task.started_event_id.unwrap(),
                            attempt: task.attempt,
                            shard_epoch: ShardEpoch::ZERO,
                        },
                        identity: WorkerIdentity("exploration".into()),
                        sdk_metadata: None,
                        metering_metadata: None,
                        worker_version: None,
                        versioning_behavior: Default::default(),
                        deployment_version: None,
                        worker_deployment_name: None,
                        sticky: None,
                        commands,
                        force_new_workflow_task: false,
                        limits: Default::default(),
                        delivered_update_ids: Vec::new(),
                        request: RequestContext::unattributed(start.now),
                        now: start.now,
                        command_sizes: Vec::new(),
                    }),
                    successor_plan,
                )
                .await
                .unwrap(),
        )
    }

    fn child_command(&self, bytes: Vec<u8>) -> WorkflowCommand {
        WorkflowCommand::StartChildWorkflow {
            child_workflow_id: self.target.workflow_id.clone(),
            namespace_id: self.target.namespace_id,
            namespace: None,
            workflow_type: self.target.workflow_type.clone(),
            task_queue: self.target.task_queue.clone(),
            input: Payloads(vec![Payload::new(bytes)]),
            header: None,
            memo: Memo::default(),
            search_attributes: SearchAttributes::default(),
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            workflow_task_timeout: Duration::seconds(10),
            retry_policy: None,
            cron_schedule: None,
            parent_close_policy: ParentClosePolicy::Terminate,
            reuse_policy: WorkflowIdReusePolicy::RejectDuplicate,
            priority: None,
        }
    }

    async fn restart(&self) -> TokeiraRuntime<Repository> {
        self.restart_with_nexus(Arc::new(NexusClient::default()))
            .await
    }

    async fn restart_with_nexus(&self, client: Arc<NexusClient>) -> TokeiraRuntime<Repository> {
        let endpoints = Arc::new(InMemoryNexusEndpointStore::new());
        endpoints
            .create(
                NexusEndpointSpec {
                    name: "exploration".into(),
                    description: Vec::new(),
                    target: NexusEndpointSpecTarget::External {
                        url: "https://nexus.example.test".into(),
                    },
                },
                0,
            )
            .unwrap();
        let runtime = TokeiraRuntime::new_with_nexus_config(
            self.repo.clone(),
            RuntimeConfig::default(),
            NexusEndpointRegistry::new(endpoints),
            client,
            NexusCompletionDeps::default(),
        );
        runtime.ready().await.unwrap();
        // Advance the injected scheduler, never wall-clock sleeps. Each round
        // gives bounded scanners and their memory-store submissions an opportunity.
        for _ in 0..8 {
            tokio::time::advance(std::time::Duration::from_secs(1)).await;
            for _ in 0..16 {
                tokio::task::yield_now().await;
            }
        }
        runtime
    }

    async fn create_confirmed_child(&mut self) {
        self.create_confirmed_child_with_policy(ParentClosePolicy::Terminate)
            .await;
    }

    async fn create_confirmed_child_with_policy(&mut self, policy: ParentClosePolicy) {
        let mut command = self.child_command(Vec::new());
        if let WorkflowCommand::StartChildWorkflow {
            parent_close_policy,
            ..
        } = &mut command
        {
            *parent_close_policy = policy;
        }
        let parent = self.complete(&self.source, vec![command]).await;
        let child = parent.children.get(&self.target.workflow_id).unwrap();
        self.target.parent_run_key = Some(self.source.run_key);
        self.target.parent_run_id = Some(self.source.run_id);
        self.target.parent_workflow_id = Some(self.source.workflow_id.clone());
        self.target.parent_namespace_id = Some(self.source.namespace_id);
        self.target.parent_initiated_event_id = child.initiated_event_id;
        applied(
            self.lane
                .submit(self.target.run_key, Command::Start(self.target.clone()))
                .await
                .unwrap(),
        );
        let confirmed = applied(
            self.lane
                .submit(
                    self.source.run_key,
                    Command::ChildStartConfirmed(ChildStartConfirmedRequest {
                        child_workflow_id: self.target.workflow_id.clone(),
                        initiated_event_id: child.initiated_event_id,
                        result: ChildStartResult::Started {
                            child_run_id: self.target.run_id,
                            workflow_type: self.target.workflow_type.clone(),
                        },
                        now: self.source.now,
                    }),
                )
                .await
                .unwrap(),
        );
        let proof = &confirmed.outbound.as_ref().unwrap().child_releases[&child.initiated_event_id];
        applied(
            self.lane
                .submit(
                    self.target.run_key,
                    Command::ParentChildStartRecorded(
                        tokeira_kernel::ParentChildStartRecordedRequest {
                            parent_run_key: self.source.run_key,
                            initiating_event: child.initiated_event_id,
                            first_child_run_id: self.target.run_id,
                            parent_started_event_id: proof.parent_started_event_id.unwrap(),
                            now: self.source.now,
                        },
                    ),
                )
                .await
                .unwrap(),
        );
    }
}

fn check_external_delivery_survives_lost_publication(
    seed: u64,
    cancel: bool,
    cuts: u8,
) -> proptest::test_runner::TestCaseResult {
    executor().block_on(async {
        let fixture = Fixture::new(seed).await;
        applied(
            fixture
                .lane
                .submit(
                    fixture.target.run_key,
                    Command::Start(fixture.target.clone()),
                )
                .await
                .unwrap(),
        );
        let command = if cancel {
            WorkflowCommand::RequestCancelExternalWorkflowExecution {
                target_namespace_id: fixture.target.namespace_id,
                target_namespace: None,
                target_workflow_id: fixture.target.workflow_id.clone(),
                target_run_id: None,
                control: "control".into(),
            }
        } else {
            WorkflowCommand::SignalExternalWorkflowExecution {
                target_namespace_id: fixture.target.namespace_id,
                target_namespace: None,
                target_workflow_id: fixture.target.workflow_id.clone(),
                target_run_id: None,
                signal_name: "recover".into(),
                input: Payloads::default(),
                header: None,
                control: "control".into(),
            }
        };
        fixture.complete(&fixture.source, vec![command]).await;
        fixture.repo.lose_outbound_reply.store(
            if cuts & 1 != 0 { 8 } else { 0 } | if cuts & 2 != 0 { 32 } else { 0 },
            Ordering::SeqCst,
        );
        fixture
            .repo
            .fail_outbound_before
            .store(if cuts & 4 != 0 { 32 } else { 0 }, Ordering::SeqCst);
        let _restarted = fixture.restart().await;
        let history = fixture
            .repo
            .read_history(fixture.target.run_key, 0, 64)
            .await
            .unwrap();
        let delivered = history
            .iter()
            .filter(|event| {
                if cancel {
                    matches!(
                        event.kind,
                        HistoryEventKind::WorkflowExecutionCancelRequested { .. }
                    )
                } else {
                    matches!(
                        event.kind,
                        HistoryEventKind::WorkflowExecutionSignaled { .. }
                    )
                }
            })
            .count();
        prop_assert_eq!(
            delivered,
            1,
            "committed external operation was lost after restart (cancel={})",
            cancel
        );
        let source = state(&fixture.repo, fixture.source.run_key).await;
        prop_assert!(
            source.pending_external_signals.is_empty()
                && source.pending_external_cancels.is_empty()
        );
        Ok(())
    })
}

fn check_untimed_nexus_survives_lost_publication(
    seed: u64,
    cancel: bool,
    cuts: u8,
) -> proptest::test_runner::TestCaseResult {
    executor().block_on(async {
        let fixture = Fixture::new(seed).await;
        let pending = fixture
            .complete(
                &fixture.source,
                vec![WorkflowCommand::ScheduleNexusOperation {
                    operation_id: "operation".into(),
                    endpoint: "exploration".into(),
                    service: "service".into(),
                    operation: "operation".into(),
                    input: Payloads::default(),
                    schedule_to_close_timeout: None,
                    schedule_to_start_timeout: None,
                    start_to_close_timeout: None,
                }],
            )
            .await;
        if cancel {
            let scheduled_event_id =
                pending.pending_nexus_operations["operation"].scheduled_event_id;
            applied(
                fixture
                    .lane
                    .submit(
                        fixture.source.run_key,
                        Command::NexusOperationResolved(NexusOperationResolvedRequest {
                            expected_attempt: None,
                            operation_id: "operation".into(),
                            scheduled_event_id,
                            resolution: NexusResolution::Started {
                                operation_token: "token".into(),
                                links: Vec::new(),
                            },
                            now: fixture.source.now,
                        }),
                    )
                    .await
                    .unwrap(),
            );
            fixture
                .complete(
                    &fixture.source,
                    vec![WorkflowCommand::CancelNexusOperation { scheduled_event_id }],
                )
                .await;
        }
        let client = Arc::new(NexusClient::default());
        let bit = if cancel { 128 } else { 64 };
        fixture
            .repo
            .lose_outbound_reply
            .store(if cuts & 1 != 0 { bit } else { 0 }, Ordering::SeqCst);
        fixture
            .repo
            .fail_outbound_before
            .store(if cuts & 2 != 0 { bit } else { 0 }, Ordering::SeqCst);
        let _restarted = fixture.restart_with_nexus(client.clone()).await;
        let attempts = if cancel {
            client.cancels.load(Ordering::SeqCst)
        } else {
            client.starts.load(Ordering::SeqCst)
        };
        prop_assert!(
            attempts > 0,
            "untimed Nexus first attempt was lost (cancel={})",
            cancel
        );
        let source = state(&fixture.repo, fixture.source.run_key).await;
        if cancel {
            prop_assert_eq!(
                source.pending_nexus_operations["operation"]
                    .cancellation
                    .as_ref()
                    .unwrap()
                    .state,
                tokeira_kernel::NexusOperationCancellationState::Succeeded
            );
        } else {
            prop_assert!(source.pending_nexus_operations.is_empty());
        }
        Ok(())
    })
}

fn check_close_obligation_survives_lost_publication(
    seed: u64,
    close_child: bool,
    retirement_loss: u8,
) -> proptest::test_runner::TestCaseResult {
    executor().block_on(async {
        let mut fixture = Fixture::new(seed).await;
        fixture.create_confirmed_child().await;
        let closing = if close_child {
            &fixture.target
        } else {
            &fixture.source
        };
        fixture
            .complete(
                closing,
                vec![WorkflowCommand::CompleteWorkflow {
                    result: Payloads::default(),
                }],
            )
            .await;
        fixture.repo.lose_outbound_reply.store(16, Ordering::SeqCst);
        fixture
            .repo
            .lose_retirement
            .store(retirement_loss, Ordering::SeqCst);
        let _restarted = fixture.restart().await;
        let settled = if close_child {
            fixture
                .repo
                .read_history(fixture.source.run_key, 0, 64)
                .await
                .unwrap()
                .iter()
                .any(|event| {
                    matches!(
                        event.kind,
                        HistoryEventKind::ChildWorkflowExecutionCompleted { .. }
                    )
                })
        } else {
            state(&fixture.repo, fixture.target.run_key).await.status
                == tokeira_types::ExecutionStatus::Terminated
        };
        prop_assert!(
            settled,
            "committed close obligation was lost (child-result={})",
            close_child
        );
        prop_assert!(
            fixture
                .repo
                .read_intent_page(ShardId(0), tokeira_storage::ScanBound::Capture, None, 100)
                .await
                .unwrap()
                .rows
                .is_empty()
        );
        let history = fixture
            .repo
            .read_history(
                if close_child {
                    fixture.source.run_key
                } else {
                    fixture.target.run_key
                },
                0,
                100,
            )
            .await
            .unwrap();
        prop_assert_eq!(
            history
                .iter()
                .filter(|event| if close_child {
                    matches!(
                        event.kind,
                        HistoryEventKind::ChildWorkflowExecutionCompleted { .. }
                    )
                } else {
                    matches!(
                        event.kind,
                        HistoryEventKind::WorkflowExecutionTerminated { .. }
                    )
                })
                .count(),
            1
        );
        Ok(())
    })
}

fn check_refused_reset_leaves_all_participants_unchanged(
    seed: u64,
    distinct_current: bool,
) -> proptest::test_runner::TestCaseResult {
    executor().block_on(async {
        let fixture = Fixture::new(seed).await;
        fixture
            .complete(
                &fixture.source,
                if distinct_current {
                    vec![WorkflowCommand::CompleteWorkflow {
                        result: Payloads::default(),
                    }]
                } else {
                    Vec::new()
                },
            )
            .await;
        let mut current = fixture.source.clone();
        if distinct_current {
            current.run_id = RunId(uuid::Uuid::from_u128(u128::from(seed) * 8 + 3));
            current.run_key =
                RunKey::derive(current.namespace_id, &current.workflow_id, current.run_id);
            current.request.request_id.0 = "new-current".into();
            applied(
                fixture
                    .lane
                    .submit(current.run_key, Command::Start(current.clone()))
                    .await
                    .unwrap(),
            );
        }
        let runtime = fixture.restart().await;
        let before_base = fixture.repo.load_run(fixture.source.run_key).await.unwrap();
        let before_current = fixture.repo.load_run(current.run_key).await.unwrap();
        let history = fixture
            .repo
            .read_history(fixture.source.run_key, 0, 64)
            .await
            .unwrap();
        let fork_event_id = history
            .iter()
            .find(|event| matches!(event.kind, HistoryEventKind::WorkflowTaskCompleted { .. }))
            .unwrap()
            .event_id;
        fixture.repo.fail_reset.store(true, Ordering::SeqCst);
        let result = runtime
            .reset_workflow(
                ExecutionRef {
                    namespace_id: fixture.source.namespace_id,
                    workflow_id: fixture.source.workflow_id.clone(),
                    run_id: Some(fixture.source.run_id),
                },
                tokeira_kernel::ResetRequest {
                    fork_event_id,
                    new_run_id: RunId(uuid::Uuid::from_u128(u128::from(seed) * 8 + 4)),
                    reapply_exclude_signal: false,
                    reapply_exclude_update: false,
                    post_reset_versioning_overrides: Vec::new(),
                    expected_current_run_key: None,
                    reason: "exploration".into(),
                    request: RequestContext::unattributed(fixture.source.now),
                    now: fixture.source.now,
                },
            )
            .await;
        prop_assert!(
            result.is_err(),
            "injected reset preparation failure must reach caller"
        );
        prop_assert_eq!(
            fixture.repo.load_run(fixture.source.run_key).await.unwrap(),
            before_base,
            "failed reset changed base before new branch could publish"
        );
        prop_assert_eq!(
            fixture.repo.load_run(current.run_key).await.unwrap(),
            before_current
        );
        Ok(())
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: outbound-dispatch-recovery, Property 9: Callback recovery preservation.
    #[test]
    fn callback_recovery_preserves_payload_deadline_and_one_attempt_authority(seed in any::<u64>(), bytes in proptest::collection::vec(any::<u8>(), 0..256), phase in 0u8..5, failed in any::<bool>()) {
        executor().block_on(async {
            let mut fixture = Fixture::new(seed).await;
            fixture.target.completion_callbacks = vec![tokeira_kernel::CompletionCallback {
                spec: tokeira_kernel::CallbackSpec::Nexus { url: "https://callback.example.test".into(), header: Default::default() },
                links: Vec::new(), trigger: tokeira_kernel::CallbackTrigger::WorkflowClosed, registration_time: None,
                state: tokeira_kernel::CallbackState::Standby, attempt: 0, last_attempt_failure: None,
                last_attempt_complete_time: None, next_attempt_at: None,
            }];
            applied(fixture.lane.submit(fixture.target.run_key, Command::Start(fixture.target.clone())).await.unwrap());
            let payload = Payload::new(bytes);
            fixture.complete(&fixture.target, vec![if failed { WorkflowCommand::FailWorkflow { failure: payload.clone() } }
                else { WorkflowCommand::CompleteWorkflow { result: Payloads(vec![payload.clone()]) } }]).await;
            if phase != 0 {
                let outcome = match phase {
                    1 | 2 => tokeira_kernel::CallbackAttemptOutcome::RetryableFailure {
                        failure: Payload::new(Vec::new()), next_attempt_at: OffsetDateTime::now_utc() + if phase == 1 { Duration::hours(-1) } else { Duration::hours(1) },
                    },
                    3 => tokeira_kernel::CallbackAttemptOutcome::Succeeded,
                    _ => tokeira_kernel::CallbackAttemptOutcome::NonRetryableFailure { failure: Payload::new(Vec::new()) },
                };
                applied(fixture.lane.submit(fixture.target.run_key, Command::CompletionCallbackAttempted(tokeira_kernel::CompletionCallbackAttemptedRequest {
                    callback_index: 0, outcome, now: OffsetDateTime::now_utc(),
                })).await.unwrap());
            }
            let before = state(&fixture.repo, fixture.target.run_key).await.completion_callbacks[0].clone();
            let client = Arc::new(HeldCallbackClient::default());
            let runtime = TokeiraRuntime::new_with_nexus_config(fixture.repo.clone(), RuntimeConfig::default(),
                NexusEndpointRegistry::default(), Arc::new(NexusClient::default()), NexusCompletionDeps {
                    client: client.clone(), scanner: crate::nexus::CompletionCallbackScannerConfig { scan_interval: std::time::Duration::from_millis(10), max_per_scan: 100 },
                    ..Default::default()
                });
            runtime.ready().await.unwrap();
            for _ in 0..24 {
                tokio::time::advance(std::time::Duration::from_millis(10)).await;
                for _ in 0..16 { tokio::task::yield_now().await; }
            }
            let due = phase <= 1;
            prop_assert_eq!(client.calls.lock().unwrap().len(), usize::from(due), "repeated scans must not overlap a pending callback HTTP attempt");
            if due {
                let expected = if failed { crate::nexus::NexusCompletion::Failed(crate::nexus::NexusCompletionFailureBody {
                    message: "operation failed".into(), failure: payload,
                }.encode()) } else { crate::nexus::NexusCompletion::Succeeded(Payloads(vec![payload])) };
                prop_assert_eq!(client.calls.lock().unwrap()[0].clone(), expected);
                client.release.notify_one();
                for _ in 0..128 { tokio::task::yield_now().await; }
                let callback = state(&fixture.repo, fixture.target.run_key).await.completion_callbacks[0].clone();
                prop_assert_eq!(callback.state, tokeira_kernel::CallbackState::Succeeded);
                prop_assert_eq!(callback.attempt, before.attempt + 1);
            } else {
                prop_assert_eq!(state(&fixture.repo, fixture.target.run_key).await.completion_callbacks[0].clone(), before);
            }
            let shutdown = runtime.shutdown_handle(); shutdown.begin_shutdown(); drop(runtime);
            shutdown.wait(std::time::Instant::now() + std::time::Duration::from_secs(5)).await.unwrap();
            Ok(())
        })?;
    }

    // Feature: outbound-dispatch-recovery, Property 20: End-to-end crash refinement
    #[test]
    fn child_start_survives_lost_publication(seed in any::<u64>(), bytes in proptest::collection::vec(any::<u8>(), 0..64), lost_replies in 0u8..16) {
        executor().block_on(async {
            let fixture = Fixture::new(seed).await;
            let pending = fixture.complete(&fixture.source, vec![fixture.child_command(bytes.clone())]).await;
            assert_eq!(pending.children.len(), 1);
            fixture.repo.lose_outbound_reply.store(lost_replies, Ordering::SeqCst);
            fixture.repo.lose_child_seed_reply.store(lost_replies & 8 != 0, Ordering::SeqCst);
            let _restarted = fixture.restart().await;
            let child = fixture.repo.resolve_execution(&ExecutionRef {
                namespace_id: fixture.target.namespace_id,
                workflow_id: fixture.target.workflow_id.clone(),
                run_id: None,
            }).await.unwrap();
            prop_assert!(child.is_some(), "committed child initiation was lost after restart");
            let LoadedRun::Existing(child) = fixture.repo.load_run(child.unwrap()).await.unwrap() else { panic!("created child missing"); };
            let LoadedRun::Existing(parent) = fixture.repo.load_run(fixture.source.run_key).await.unwrap() else { panic!("parent missing"); };
            let initiation = pending.children[&fixture.target.workflow_id].initiated_event_id;
            let selected = &parent.outbound.as_ref().unwrap().child_seeds[&initiation];
            prop_assert_eq!(selected.child_run_id, child.run_id);
            prop_assert!(child.outbound.as_ref().unwrap().child_gate.as_ref().unwrap().released);
            prop_assert!(parent.outbound.as_ref().unwrap().child_releases.is_empty());
            let history = fixture.repo.read_history(child.run_key, 0, 64).await.unwrap();
            let starts: Vec<_> = history.iter().filter_map(|event| match &event.kind {
                HistoryEventKind::WorkflowExecutionStartedV2 { input, .. } => Some(input), _ => None,
            }).collect();
            prop_assert_eq!(starts.len(), 1);
            prop_assert_eq!(starts[0], &Payloads(vec![Payload::new(bytes)]));
            Ok(())
        })?;
    }

    // Feature: outbound-dispatch-recovery, Property 20: End-to-end crash refinement
    #[test]
    fn external_signal_survives_lost_publication(seed in any::<u64>(), cuts in 0u8..8) {
        check_external_delivery_survives_lost_publication(seed, false, cuts)?;
    }

    // Feature: outbound-dispatch-recovery, Property 20: End-to-end crash refinement
    #[test]
    fn external_cancel_survives_lost_publication(seed in any::<u64>(), cuts in 0u8..8) {
        check_external_delivery_survives_lost_publication(seed, true, cuts)?;
    }

    // Feature: outbound-dispatch-recovery, Property 6: Child start precedes execution exactly once
    #[test]
    fn child_confirmation_is_idempotent(seed in any::<u64>(), copies in 2usize..5) {
        executor().block_on(async {
            let fixture = Fixture::new(seed).await;
            let pending = fixture.complete(&fixture.source, vec![fixture.child_command(Vec::new())]).await;
            let child = pending.children.get(&fixture.target.workflow_id).unwrap();
            let confirmed = ChildStartConfirmedRequest {
                child_workflow_id: fixture.target.workflow_id.clone(),
                initiated_event_id: child.initiated_event_id,
                result: ChildStartResult::Started { child_run_id: fixture.target.run_id, workflow_type: fixture.target.workflow_type.clone() },
                now: fixture.source.now,
            };
            for _ in 0..copies {
                fixture.lane.submit(fixture.source.run_key, Command::ChildStartConfirmed(confirmed.clone())).await.unwrap();
            }
            let history = fixture.repo.read_history(fixture.source.run_key, 0, 64).await.unwrap();
            let count = history.iter().filter(|event| matches!(event.kind, HistoryEventKind::ChildWorkflowExecutionStarted { .. })).count();
            prop_assert_eq!(count, 1, "duplicate child confirmation authored duplicate history");
            Ok(())
        })?;
    }

    // Feature: outbound-dispatch-recovery, Property 20: End-to-end crash refinement
    #[test]
    fn untimed_nexus_start_survives_lost_publication(seed in any::<u64>(), cuts in 0u8..4) {
        check_untimed_nexus_survives_lost_publication(seed, false, cuts)?;
    }

    // Feature: outbound-dispatch-recovery, Property 20: End-to-end crash refinement
    #[test]
    fn untimed_nexus_cancel_survives_lost_publication(seed in any::<u64>(), cuts in 0u8..4) {
        check_untimed_nexus_survives_lost_publication(seed, true, cuts)?;
    }

    // Feature: outbound-dispatch-recovery, Property 20: End-to-end crash refinement
    #[test]
    fn parent_close_survives_lost_publication(seed in any::<u64>(), retirement_loss in 0u8..4) {
        check_close_obligation_survives_lost_publication(seed, false, retirement_loss)?;
    }

    // Feature: outbound-dispatch-recovery, Property 10: Parent-close actions stay within the child chain.
    #[test]
    fn parent_close_checks_first_run_parent_and_initiation(seed in any::<u64>(), policy in 0u8..3, lineage in 0u8..5, lost_reply in any::<bool>()) {
        executor().block_on(async {
            let mut fixture = Fixture::new(seed).await;
            let policy = match policy { 0 => ParentClosePolicy::Abandon, 1 => ParentClosePolicy::RequestCancel, _ => ParentClosePolicy::Terminate };
            fixture.create_confirmed_child_with_policy(policy).await;
            if lineage != 0 {
                fixture.complete(&fixture.target, vec![WorkflowCommand::CompleteWorkflow { result: Payloads::default() }]).await;
                let first = fixture.target.run_id;
                fixture.target.run_id = RunId::new();
                fixture.target.run_key = RunKey::derive(fixture.target.namespace_id, &fixture.target.workflow_id, fixture.target.run_id);
                fixture.target.request.request_id.0 = "next-child".into();
                fixture.target.first_execution_run_id = Some(if lineage == 2 { RunId::new() } else { first });
                fixture.target.continued_execution_run_id = Some(first);
                if lineage == 3 { fixture.target.parent_run_key = Some(RunKey::new()); fixture.target.parent_run_id = Some(RunId::new()); }
                if lineage == 4 { fixture.target.parent_initiated_event_id += 1; }
                applied(fixture.lane.submit(fixture.target.run_key, Command::Start(fixture.target.clone())).await.unwrap());
            }
            fixture.complete(&fixture.source, vec![WorkflowCommand::CompleteWorkflow { result: Payloads::default() }]).await;
            fixture.repo.lose_outbound_reply.store(if lost_reply { 16 } else { 0 }, Ordering::SeqCst);
            let runtime = fixture.restart().await;
            let target = state(&fixture.repo, fixture.target.run_key).await;
            let matching = lineage < 2;
            prop_assert_eq!(target.status == tokeira_types::ExecutionStatus::Terminated, matching && policy == ParentClosePolicy::Terminate);
            prop_assert_eq!(target.cancel_requested, matching && policy == ParentClosePolicy::RequestCancel);
            let history = fixture.repo.read_history(target.run_key, 0, 100).await.unwrap();
            prop_assert!(history.iter().filter(|event| matches!(event.kind, HistoryEventKind::WorkflowExecutionTerminated { .. } | HistoryEventKind::WorkflowExecutionCancelRequested { .. })).count() <= 1, "at most one close-policy effect");
            prop_assert!(fixture.repo.read_intent_page(ShardId(0), tokeira_storage::ScanBound::Capture, None, 100).await.unwrap().rows.is_empty());
            let shutdown = runtime.shutdown_handle(); shutdown.begin_shutdown(); drop(runtime);
            shutdown.wait(std::time::Instant::now() + std::time::Duration::from_secs(5)).await.unwrap();
            Ok(())
        })?;
    }

    // Feature: outbound-dispatch-recovery, Property 20: End-to-end crash refinement
    #[test]
    fn child_result_survives_lost_publication(seed in any::<u64>(), retirement_loss in 0u8..4) {
        check_close_obligation_survives_lost_publication(seed, true, retirement_loss)?;
    }

    // Feature: outbound-dispatch-recovery, Property 6: Child start precedes execution exactly once
    #[test]
    fn child_first_task_waits_for_parent_confirmation(seed in any::<u64>()) {
        executor().block_on(async {
            let mut fixture = Fixture::new(seed).await;
            let parent = fixture.complete(&fixture.source, vec![fixture.child_command(Vec::new())]).await;
            fixture.target.parent_run_key = Some(fixture.source.run_key);
            fixture.target.parent_run_id = Some(fixture.source.run_id);
            fixture.target.parent_workflow_id = Some(fixture.source.workflow_id.clone());
            fixture.target.parent_namespace_id = Some(fixture.source.namespace_id);
            fixture.target.parent_initiated_event_id = parent.children[&fixture.target.workflow_id].initiated_event_id;
            applied(fixture.lane.submit(fixture.target.run_key, Command::Start(fixture.target.clone())).await.unwrap());
            let tasks = fixture.repo.list_dispatchable_workflow_tasks(&tokeira_types::QueueKey {
                namespace_id: fixture.target.namespace_id,
                task_queue: fixture.target.task_queue.clone(),
                task_kind: tokeira_types::TaskKind::Workflow,
                deployment: None,
                build_id: None,
            }, 100).await.unwrap();
            prop_assert!(tasks.is_empty(), "child first task became runnable before parent recorded its start");
            Ok(())
        })?;
    }

    // Feature: outbound-dispatch-recovery, Property 12: Reset publication is all or nothing
    #[test]
    fn complete_reset_is_visible_and_retry_uses_published_identity(seed in any::<u64>(), distinct in any::<bool>(), lost_reply in any::<bool>()) {
        executor().block_on(async {
            let fixture = Fixture::new(seed).await;
            fixture.complete(&fixture.source, if distinct { vec![WorkflowCommand::CompleteWorkflow { result: Payloads::default() }] } else { Vec::new() }).await;
            let mut current = fixture.source.clone();
            if distinct {
                current.run_id = RunId(uuid::Uuid::from_u128(u128::from(seed) * 8 + 3));
                current.run_key = RunKey::derive(current.namespace_id, &current.workflow_id, current.run_id);
                current.request.request_id.0 = "current".into();
                applied(fixture.lane.submit(current.run_key, Command::Start(current.clone())).await.unwrap());
            }
            let before_base = state(&fixture.repo, fixture.source.run_key).await;
            let prefix = fixture.repo.read_attributed_history(fixture.source.run_key, 0, 3).await.unwrap();
            let runtime = fixture.restart().await;
            let execution = ExecutionRef { namespace_id: fixture.source.namespace_id, workflow_id: fixture.source.workflow_id.clone(), run_id: Some(fixture.source.run_id) };
            let mut context = RequestContext::unattributed(fixture.source.now);
            context.request_id.0 = format!("reset-request-{seed}");
            let reset = tokeira_kernel::ResetRequest {
                fork_event_id: 4, new_run_id: RunId(uuid::Uuid::from_u128(u128::from(seed) * 8 + 4)),
                reapply_exclude_signal: false, reapply_exclude_update: false,
                post_reset_versioning_overrides: Vec::new(), expected_current_run_key: None,
                reason: "reset".into(), request: context, now: OffsetDateTime::now_utc(),
            };
            fixture.repo.lose_publication_reply.store(lost_reply, Ordering::SeqCst);
            let first = runtime.reset_workflow(execution.clone(), reset.clone()).await;
            prop_assert_eq!(first.is_err(), lost_reply);
            let mut retry = reset.clone(); retry.new_run_id = RunId::new();
            let result = runtime.reset_workflow(execution, retry).await.unwrap();
            prop_assert_eq!(result.successor_run_id, reset.new_run_id);
            let successor = state(&fixture.repo, result.successor_run_key).await;
            prop_assert!(successor.pending_workflow_task.as_ref().unwrap().started_event_id.is_none());
            prop_assert_eq!(successor.outbound.as_ref().unwrap().reset_request_id.as_ref(), Some(&reset.request.request_id));
            prop_assert_eq!(fixture.repo.find_latest_run(current.namespace_id, &current.workflow_id).await.unwrap(), Some(successor.run_key));
            prop_assert!(!state(&fixture.repo, current.run_key).await.is_open());
            let base = state(&fixture.repo, fixture.source.run_key).await;
            prop_assert_eq!(base.reset_run_id, Some(reset.new_run_id));
            if distinct { prop_assert_eq!(base.last_event_id, before_base.last_event_id); }
            let history = fixture.repo.read_attributed_history_to_end(successor.run_key, 0).await.unwrap();
            prop_assert_eq!(&history[..3], prefix.as_slice());
            prop_assert!(matches!(history[3].event.kind, HistoryEventKind::WorkflowTaskFailed { failure_cause: tokeira_kernel::WorkflowTaskFailedCause::ResetWorkflow, .. }), "expected reset event order");
            prop_assert!(matches!(history.last().unwrap().event.kind, HistoryEventKind::WorkflowTaskScheduled { .. }), "expected reset event order");
            let shutdown = runtime.shutdown_handle(); shutdown.begin_shutdown(); drop(runtime);
            shutdown.wait(std::time::Instant::now() + std::time::Duration::from_secs(5)).await.unwrap();
            Ok(())
        })?;
    }

    // Feature: outbound-dispatch-recovery, Property 13: Reset retry preserves request and semantic order.
    #[test]
    fn reset_reconnects_child_and_retains_deleted_redirect_until_result(seed in any::<u64>(), delete_base in any::<bool>(), lost_reply in any::<bool>()) {
        executor().block_on(async {
            let mut fixture = Fixture::new(seed).await;
            fixture.create_confirmed_child().await;
            fixture.complete(&fixture.source, Vec::new()).await;
            let history = fixture.repo.read_history(fixture.source.run_key, 0, 100).await.unwrap();
            let fork_event_id = history.iter().rev().find(|event| matches!(event.kind, HistoryEventKind::WorkflowTaskCompleted { .. })).unwrap().event_id;
            let runtime = fixture.restart().await;
            let mut context = RequestContext::unattributed(fixture.source.now);
            context.request_id.0 = format!("reset-child-{seed}");
            let reset = tokeira_kernel::ResetRequest {
                fork_event_id, new_run_id: RunId::new(), reapply_exclude_signal: false, reapply_exclude_update: false,
                post_reset_versioning_overrides: Vec::new(), expected_current_run_key: None,
                reason: "reset child lineage".into(), request: context, now: OffsetDateTime::now_utc(),
            };
            let execution = ExecutionRef { namespace_id: fixture.source.namespace_id, workflow_id: fixture.source.workflow_id.clone(), run_id: Some(fixture.source.run_id) };
            fixture.repo.lose_publication_reply.store(lost_reply, Ordering::SeqCst);
            let first = runtime.reset_workflow(execution.clone(), reset.clone()).await;
            prop_assert_eq!(first.is_err(), lost_reply);
            let published = runtime.reset_workflow(execution, reset.clone()).await.unwrap();
            prop_assert_eq!(published.successor_run_id, reset.new_run_id);
            let parent = state(&fixture.repo, published.successor_run_key).await;
            prop_assert_eq!(parent.children[&fixture.target.workflow_id].child_run_id, Some(fixture.target.run_id));
            prop_assert!(state(&fixture.repo, fixture.target.run_key).await.is_open());
            if delete_base {
                let base = state(&fixture.repo, fixture.source.run_key).await;
                fixture.repo.delete_run_for_bundle(base.run_key, ShardId(0), tokeira_storage::DeleteRunRequest {
                    expected_seq: base.transition_seq, deleted_at: OffsetDateTime::now_utc(),
                }, tokeira_storage::WriteFence::Unfenced).await.unwrap();
                fixture.repo.purge_run(base.run_key).await.unwrap();
                prop_assert!(matches!(fixture.repo.load_outbound_source(base.run_key).await.unwrap(), LoadedRun::Existing(_)));
            }
            fixture.complete(&fixture.target, vec![WorkflowCommand::CompleteWorkflow { result: Payloads(vec![Payload::new("child result")]) }]).await;
            fixture.repo.lose_outbound_reply.store(16, Ordering::SeqCst);
            for _ in 0..12 {
                tokio::time::advance(std::time::Duration::from_secs(1)).await;
                for _ in 0..32 { tokio::task::yield_now().await; }
            }
            let history = fixture.repo.read_history(parent.run_key, 0, 100).await.unwrap();
            let outcomes = history.iter().filter(|event| matches!(event.kind, HistoryEventKind::ChildWorkflowExecutionCompleted { .. })).count();
            prop_assert_eq!(outcomes, 1);
            prop_assert!(state(&fixture.repo, parent.run_key).await.children.is_empty());
            prop_assert_eq!(fixture.repo.find_latest_run(fixture.target.namespace_id, &fixture.target.workflow_id).await.unwrap(), Some(fixture.target.run_key));
            if delete_base {
                fixture.repo.purge_run(fixture.source.run_key).await.unwrap();
                prop_assert_eq!(fixture.repo.load_outbound_source(fixture.source.run_key).await.unwrap(), LoadedRun::Absent);
            }
            let shutdown = runtime.shutdown_handle(); shutdown.begin_shutdown(); drop(runtime);
            shutdown.wait(std::time::Instant::now() + std::time::Duration::from_secs(5)).await.unwrap();
            Ok(())
        })?;
    }

    // Feature: outbound-dispatch-recovery, Property 20: End-to-end crash refinement
    #[test]
    fn refused_reset_preserves_current_base(seed in any::<u64>()) {
        check_refused_reset_leaves_all_participants_unchanged(seed, false)?;
    }

    // Feature: outbound-dispatch-recovery, Property 20: End-to-end crash refinement
    #[test]
    fn refused_reset_preserves_distinct_current(seed in any::<u64>()) {
        check_refused_reset_leaves_all_participants_unchanged(seed, true)?;
    }

    // Feature: outbound-dispatch-recovery, Property 20: End-to-end crash refinement
    #[test]
    fn oversized_successor_survives_lost_separate_start(seed in any::<u64>(), lose_publish_reply in any::<bool>()) {
        executor().block_on(async {
            let fixture = Fixture::new(seed).await;
            let successor = RunId(uuid::Uuid::from_u128(u128::from(seed) * 8 + 5));
            let key = RunKey::derive(fixture.source.namespace_id, &fixture.source.workflow_id, successor);
            fixture.repo.refuse_pair.store(true, Ordering::SeqCst);
            fixture.repo.lose_publication_reply.store(lose_publish_reply, Ordering::SeqCst);
            fixture.complete(&fixture.source, vec![WorkflowCommand::ContinueAsNew {
                new_run_id: successor, workflow_type: fixture.source.workflow_type.clone(),
                task_queue: fixture.source.task_queue.clone(), input: Payloads::default(),
                memo: Memo::default(), search_attributes: SearchAttributes::default(),
                workflow_execution_timeout: None, workflow_run_timeout: Some(Duration::hours(1)),
                workflow_task_timeout: fixture.source.workflow_task_timeout,
                retry_policy: None, header: None, initial_versioning_behavior: Default::default(),
                successor_versioning_info: None,
            }]).await;
            prop_assert!(!state(&fixture.repo, fixture.source.run_key).await.status.is_open());
            let restarted = fixture.restart().await;
            prop_assert!(matches!(fixture.repo.load_run(key).await.unwrap(), LoadedRun::Existing(_)),
                "size fallback closed predecessor but lost its successor after restart");
            prop_assert!(restarted.workflow_timeout_tracking().snapshot().iter().any(|entry| entry.run_key == key),
                "lost publication reply must not lose successor deadlines after intent retirement");
            Ok(())
        })?;
    }
}
