//! Activity publication counterexamples across separate execution and queue homes.
//!
//! Destination brokers have no discovery driver. A manually polled waiting future
//! and completed owner operations delimit each observation; injected time drives
//! retries. Durable rows independently establish that the missing offer is due.

use std::{future::Future, num::NonZeroU32, task::Poll};

use proptest::{
    prelude::*,
    test_runner::{Config, FileFailurePersistence, RngAlgorithm, TestError, TestRng, TestRunner},
};
use tokeira_storage::{ActivityDiscoveryRange, InMemoryStore};
use tokeira_types::{WorkflowId, bundle_for_partition, workflow_queue_partition};

use super::*;
use crate::{
    activity_eligibility::scan_activity_eligibility_once,
    activity_timeout::scan_activity_timeouts_once,
    discovery::{PlacementQueueHomes, QueueHomeProvider},
    test_clock::TestClock,
};

#[derive(Clone, Copy, Debug)]
enum Path {
    Schedule,
    FailureBackoff,
    FailureDue,
    Timeout,
    ActivityUnpause,
    ActivityReset,
    WorkflowUnpause,
    QueueMove,
    WorkflowReset,
    Recovery,
}

fn home(queue: &QueueKey) -> ShardId {
    bundle_for_partition(
        workflow_queue_partition(queue.namespace_id, &queue.task_queue.0, 8),
        8,
    )
}

fn destination(queue: &QueueKey) -> (InMemoryActivityBroker, Arc<PlacementQueueHomes>) {
    let mut owner = ShardOwner::new(8);
    owner.record_acquired(home(queue), ShardEpoch::ZERO);
    owner.mark_active(home(queue));
    let homes = Arc::new(PlacementQueueHomes {
        owner: Arc::new(RwLock::new(owner)),
        counts: RwLock::new((8, 8)),
    });
    (InMemoryActivityBroker::with_discovery(homes.clone()), homes)
}

fn request_context(now: OffsetDateTime) -> RequestContext {
    RequestContext {
        request_id: RequestId(Uuid::new_v4().to_string()),
        ..RequestContext::unattributed(now)
    }
}

async fn state(repo: &InMemoryStore, run: RunKey) -> WorkflowState {
    let LoadedRun::Existing(state) = repo.load_run(run).await.unwrap() else {
        panic!("committed run missing")
    };
    state
}

// Independent sink installation keeps disconnected controls unable to push.
#[derive(Debug)]
struct ActivitySink {
    fault: std::sync::atomic::AtomicU8,
    broker: InMemoryActivityBroker,
    homes: Arc<PlacementQueueHomes>,
    clock: Arc<dyn Clock>,
    acknowledgements: tokio::sync::mpsc::UnboundedSender<tokeira_storage::ActivityIncarnation>,
}
#[async_trait::async_trait]
impl crate::RemoteActivityOfferSink for ActivitySink {
    async fn offer(
        &self,
        offer: &crate::ActivityQueueOffer,
        destination: ShardId,
        _: bool,
    ) -> std::result::Result<crate::ActivityOfferOutcome, crate::ActivityOfferError> {
        let fault = self.fault.swap(0, std::sync::atomic::Ordering::SeqCst);
        match fault {
            1 => return Err(crate::ActivityOfferError::Transport),
            3 | 4 => {
                self.acknowledgements
                    .send(crate::activity_offers::incarnation(&offer.task))
                    .unwrap();
                return if fault == 3 {
                    Ok(crate::ActivityOfferOutcome::Full)
                } else {
                    Err(crate::ActivityOfferError::Unimplemented)
                };
            }
            5 => return Ok(crate::ActivityOfferOutcome::NotHome),
            _ => {}
        }
        assert_eq!(destination, home(&offer.task.queue));
        let acquisition = self.homes.local_home(&offer.task.queue).unwrap();
        let outcome = self
            .broker
            .admit_activity_offer(
                offer.clone(),
                Some(&acquisition),
                crate::workflow_offers::OfferSource::Pushed,
                self.clock.now(),
                None,
            )
            .await;
        if fault == 2 {
            return Err(crate::ActivityOfferError::Transport);
        }
        if matches!(
            outcome,
            crate::ActivityOfferOutcome::Admitted | crate::ActivityOfferOutcome::Known
        ) {
            self.acknowledgements
                .send(crate::activity_offers::incarnation(&offer.task))
                .unwrap();
        }
        Ok(outcome)
    }
}

async fn execution_owner(
    repo: Arc<InMemoryStore>,
    clock: Arc<dyn Clock>,
    eager: bool,
) -> TokeiraRuntime<InMemoryStore> {
    let mut owner = TokeiraRuntime::new_with_nexus_and_shards_and_endpoint(
        repo.clone(),
        1,
        LaneConfig {
            controller_managed_placement: true,
            ..Default::default()
        },
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        ActivityTimeoutScannerConfig::default(),
        NexusTimeoutScannerConfig::default(),
        NexusEndpointRegistry::default(),
        Arc::new(NoopNexusHttpClient),
        NexusCompletionDeps::default(),
        8,
        "execution".into(),
        "execution".into(),
        false,
        None,
        clock,
    )
    .with_activity_eager_execution(eager);
    activity_deadline_tests::stop_scanners(&mut owner).await;
    // No source-side discovery slice can supply the destination broker either.
    owner.activity_discovery.test_gate.arm();
    owner.activity_discovery.test_gate.completed();
    owner
}

async fn publication(
    path: Path,
    variant: u32,
    local: bool,
    connected: bool,
    discover: bool,
) -> bool {
    publication_with_fault(path, variant, local, connected, discover, 0).await
}

async fn publication_with_fault(
    path: Path,
    variant: u32,
    local: bool,
    connected: bool,
    discover: bool,
    fault: u8,
) -> bool {
    let clock: Arc<dyn Clock> = Arc::new(TestClock::new());
    let repo = Arc::new(InMemoryStore::with_shard_count(8).with_clock(clock.clone()));
    let eager = matches!(
        path,
        Path::FailureBackoff | Path::FailureDue | Path::Timeout
    );
    let mut owner = execution_owner(repo.clone(), clock.clone(), eager).await;
    let mut start = timer_deadline_tests::start(&owner, false);
    start.workflow_id = WorkflowId(format!("activity-delivery-{variant}"));
    start.run_id = RunId(Uuid::from_u128(u128::from(variant) + 1));
    start.run_key = RunKey::derive(start.namespace_id, &start.workflow_id, start.run_id);
    let execution = execution_home_bundle(
        start.namespace_id.0.as_bytes(),
        start.workflow_id.0.as_bytes(),
        8,
    );
    let queue_for = |prefix: &str, wanted: bool| {
        (0..256)
            .map(|i| QueueKey {
                namespace_id: start.namespace_id,
                task_queue: TaskQueueName(format!("{prefix}-{variant}-{i}")),
                task_kind: TaskKind::Activity,
                deployment: None,
                build_id: None,
            })
            .find(|queue| (home(queue) == execution) == wanted)
            .unwrap()
    };
    let selected = queue_for("activity", false);
    let original = if matches!(path, Path::QueueMove) {
        queue_for("original", true)
    } else {
        selected.clone()
    };
    let workflow_queue = QueueKey {
        task_kind: TaskKind::Workflow,
        ..queue_for("workflow", true)
    };
    start.task_queue = workflow_queue.task_queue.clone();
    owner.acquire_shard(execution).await.unwrap();
    if local {
        owner.acquire_shard(home(&selected)).await.unwrap();
    }
    let (destination, destination_homes) = if local {
        (
            owner.activity_broker.clone(),
            Arc::new(PlacementQueueHomes {
                owner: owner.shard_owner.clone(),
                counts: RwLock::new((8, 8)),
            }),
        )
    } else {
        destination(&selected)
    };
    let mut run = start.run_key;
    let execution_ref = ExecutionRef {
        namespace_id: start.namespace_id,
        workflow_id: start.workflow_id.clone(),
        run_id: Some(start.run_id),
    };
    owner.start_workflow(start).await.unwrap();
    let first = owner
        .poll_workflow_task(
            workflow_queue,
            WorkerIdentity("workflow-worker".into()),
            std::time::Duration::from_secs(1),
        )
        .await
        .unwrap()
        .unwrap();
    let mut command = completion_eager_tests::schedule("activity", true);
    let delay = Duration::milliseconds(i64::from(variant));
    if let WorkflowCommand::ScheduleActivity {
        task_queue,
        retry_policy,
        start_to_close_timeout,
        heartbeat_timeout,
        priority,
        ..
    } = &mut command
    {
        *task_queue = original.task_queue.clone();
        retry_policy.as_mut().unwrap().initial_interval = if matches!(path, Path::FailureDue) {
            Duration::ZERO
        } else {
            delay
        };
        *start_to_close_timeout = Some(Duration::seconds(1));
        *heartbeat_timeout = None;
        *priority = Some(tokeira_kernel::Priority {
            priority_key: (variant % 5 + 1) as i32,
            fairness_key: String::new(),
            fairness_weight: 0.0,
        });
    }
    let complete = |first: &StartedWorkflowTask| {
        let mut complete =
            completion_inline_tests::completion(first, matches!(path, Path::WorkflowReset));
        complete.now = clock.now();
        complete.request.received_at = complete.now;
        complete.commands = vec![command.clone()];
        complete.force_new_workflow_task = matches!(path, Path::WorkflowReset);
        complete
    };
    let mut completed = if matches!(path, Path::Schedule) {
        None
    } else {
        Some(
            owner
                .complete_workflow_task_with_result(complete(&first))
                .await
                .unwrap(),
        )
    };
    if matches!(path, Path::ActivityUnpause | Path::ActivityReset) {
        owner
            .pause_activities(
                run,
                PauseActivityRequest {
                    target: ActivityControlTarget::All,
                    identity: "control".into(),
                    reason: "pause".into(),
                    request: request_context(owner.clock.now()),
                    now: owner.clock.now(),
                    rule_id: None,
                },
            )
            .await
            .unwrap();
        assert!(
            state(&repo, run).await.activities["activity"]
                .pause_info
                .is_some()
        );
    }
    if matches!(path, Path::WorkflowUnpause) {
        owner
            .pause_workflow(
                execution_ref.clone(),
                PauseWorkflowRequest {
                    identity: "control".into(),
                    reason: "pause".into(),
                    request: request_context(owner.clock.now()),
                    now: owner.clock.now(),
                },
            )
            .await
            .unwrap();
        assert_eq!(state(&repo, run).await.status, ExecutionStatus::Paused);
    }
    let recovery_epoch = owner
        .shard_owner
        .read()
        .unwrap()
        .acquisition(execution)
        .unwrap()
        .epoch;
    if fault == 6 {
        discovery_tests::stop(&owner).await;
        drop(owner);
        owner = execution_owner(repo.clone(), clock.clone(), eager).await;
    }
    // Remove fixture publications before parking the observation poll. The
    // subsequent lifecycle action must publish its own current incarnation.
    let (acknowledgements, mut acknowledged) = tokio::sync::mpsc::unbounded_channel();
    if connected && !local {
        owner.set_activity_offer_sink(Arc::new(ActivitySink {
            fault: std::sync::atomic::AtomicU8::new(fault),
            broker: destination.clone(),
            homes: destination_homes.clone(),
            clock: owner.clock.clone(),
            acknowledgements,
        }));
    }
    owner.activity_broker.remove_run(run).await;
    let worker = WorkerIdentity("activity-worker".into());
    let poll = destination.poll_activity_offer_for_worker(
        &selected,
        &worker,
        std::time::Duration::from_secs(30),
    );
    tokio::pin!(poll);
    std::future::poll_fn(|cx| {
        assert!(poll.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert!(destination.queues_with_waiters().await.contains(&selected));
    let now = owner.clock.now();
    match path {
        // Ordinary transfer pushes after releasing the run lock:
        // service/history/transfer_queue_active_task_executor.go @ v1.31.0.
        Path::Schedule => {
            owner
                .complete_workflow_task_with_result(complete(&first))
                .await
                .unwrap();
        }
        // Retry timers push for both failure and timeout retries:
        // service/history/timer_queue_active_task_executor.go @ v1.31.0.
        Path::FailureBackoff | Path::FailureDue => {
            let task = completed.as_mut().unwrap().activity_tasks.pop().unwrap();
            let (registered, resume) = owner.activity_tracking.eligibility.pause_next_refresh();
            let failed = owner.fail_activity_task(
                task.token,
                Payload::new(Vec::new()),
                None,
                false,
                None,
                None,
                request_context(now),
            );
            let barrier = async {
                registered.await.unwrap();
                let next = state(&repo, run).await;
                assert_eq!(next.activities["activity"].attempt, 2);
                let due = next.activities["activity"]
                    .current_attempt_scheduled_at
                    .unwrap();
                assert_eq!(due <= owner.clock.now(), matches!(path, Path::FailureDue));
                if due > owner.clock.now() {
                    tokio::time::advance((due - owner.clock.now()).try_into().unwrap()).await;
                }
                resume.send(()).unwrap();
            };
            let (result, ()) = tokio::join!(failed, barrier);
            result.unwrap();
        }
        Path::Timeout => {
            assert_eq!(completed.as_ref().unwrap().activity_tasks.len(), 1);
            tokio::time::advance(
                std::time::Duration::from_secs(1) + std::time::Duration::from_nanos(1),
            )
            .await;
            scan_activity_timeouts_once(
                &owner.activity_retry_deps(),
                Some(execution),
                &owner.lanes,
                owner.lanes.len(),
                &ActivityTimeoutScannerConfig::default(),
            )
            .await;
            assert_eq!(state(&repo, run).await.activities["activity"].attempt, 2);
            tokio::time::advance(delay.try_into().unwrap()).await;
        }
        // Scheduled reset/unpause regenerates due work; running and keep-paused
        // activities do not: service/history/workflow/activity.go @ v1.31.0.
        Path::ActivityUnpause => {
            owner
                .unpause_activities(
                    run,
                    UnpauseActivitiesRequest {
                        target: ActivityControlTarget::All,
                        reset_attempts: false,
                        reset_heartbeat: false,
                        jitter: None,
                        request: request_context(now),
                        now,
                    },
                )
                .await
                .unwrap();
        }
        Path::ActivityReset => {
            owner
                .reset_activities(
                    run,
                    ResetActivitiesRequest {
                        target: ActivityControlTarget::All,
                        reset_heartbeat: false,
                        keep_paused: false,
                        jitter: None,
                        restore_original_options: false,
                        request: request_context(now),
                        now,
                    },
                )
                .await
                .unwrap();
        }
        // Resumption regenerates transfer/retry tasks:
        // service/history/workflow/mutable_state_impl.go @ v1.31.0.
        Path::WorkflowUnpause => {
            owner
                .unpause_workflow(
                    execution_ref.clone(),
                    UnpauseWorkflowRequest {
                        identity: "control".into(),
                        reason: "resume".into(),
                        request: request_context(now),
                        now,
                    },
                )
                .await
                .unwrap();
        }
        // Options changes invalidate the stamp and regenerate retry work:
        // service/history/api/updateactivityoptions/api.go @ v1.31.0.
        Path::QueueMove => {
            owner
                .update_activity_options(
                    run,
                    UpdateActivitiesOptionsRequest {
                        target: ActivityControlTarget::All,
                        task_queue: FieldChange::Set(selected.task_queue.clone()),
                        schedule_to_close_timeout: FieldChange::Unchanged,
                        schedule_to_start_timeout: FieldChange::Unchanged,
                        start_to_close_timeout: FieldChange::Unchanged,
                        heartbeat_timeout: FieldChange::Unchanged,
                        retry_policy: Default::default(),
                        priority: Default::default(),
                        restore_original_options: false,
                        request: request_context(now),
                        now,
                    },
                )
                .await
                .unwrap();
        }
        Path::WorkflowReset => {
            let second = completed.as_ref().unwrap().workflow_task.as_ref().unwrap();
            run = owner
                .reset_workflow(
                    execution_ref,
                    tokeira_kernel::ResetRequest {
                        fork_event_id: second.token.started_event_id,
                        new_run_id: RunId::new(),
                        reapply_exclude_signal: false,
                        reapply_exclude_update: false,
                        post_reset_versioning_overrides: Vec::new(),
                        expected_current_run_key: None,
                        reason: "reset".into(),
                        request: request_context(now),
                        now,
                    },
                )
                .await
                .unwrap()
                .successor_run_key;
            assert_ne!(run, first.run_key);
        }
        // Task refresh reconstructs ordinary and retry work from pending state:
        // service/history/workflow/task_refresher.go @ v1.31.0.
        Path::Recovery => {
            owner
                .recover_self_assigned_shard(execution, recovery_epoch)
                .await
                .unwrap();
        }
    }
    scan_activity_eligibility_once(&owner.activity_retry_deps(), execution, owner.clock.now())
        .await;
    let committed = state(&repo, run).await;
    let activity = &committed.activities["activity"];
    assert_eq!(committed.status, ExecutionStatus::Running);
    assert!(activity.started_at.is_none() && activity.pause_info.is_none());
    assert!(activity.current_attempt_scheduled_at.unwrap() <= owner.clock.now());
    assert_eq!(activity.task_queue, selected.task_queue);
    let range = ActivityDiscoveryRange {
        namespace_id: selected.namespace_id,
        queue_name: selected.task_queue.clone(),
    };
    let before = repo
        .list_activity_dispatch_page(
            &range,
            owner.clock.now(),
            None,
            NonZeroU32::new(64).unwrap(),
        )
        .await
        .unwrap();
    assert!(before.candidates.iter().any(|candidate| {
        let row = candidate.row.as_ref().unwrap();
        row.incarnation.run_key == run
            && row.incarnation.attempt == activity.attempt
            && row.incarnation.stamp == activity.stamp
    }));
    if connected && !local {
        loop {
            let receipt =
                tokio::time::timeout(std::time::Duration::from_secs(1), acknowledged.recv())
                    .await
                    .unwrap()
                    .unwrap();
            if receipt.run_key == run
                && receipt.attempt == activity.attempt
                && receipt.stamp == activity.stamp
            {
                break;
            }
        }
    }
    if discover {
        let source = crate::activity_discovery::ActivitySource {
            repo: repo.clone(),
            registry: Arc::new(RwLock::new(None)),
            owner: owner.shard_owner.clone(),
        };
        let acquisition = destination_homes.local_home(&selected).unwrap();
        let mut reservation = destination
            .reserve_offers(&acquisition, &selected, before.candidates.len())
            .await;
        for candidate in &before.candidates {
            if let Some(task) = source
                .resolve_offer(candidate.row.as_ref().unwrap(), owner.clock.now())
                .await
                .unwrap()
            {
                assert!(matches!(
                    destination
                        .admit_activity_offer(
                            task,
                            Some(&acquisition),
                            crate::workflow_offers::OfferSource::Discovered,
                            owner.clock.now(),
                            Some(&mut reservation)
                        )
                        .await,
                    crate::ActivityOfferOutcome::Admitted | crate::ActivityOfferOutcome::Known
                ));
            }
        }
    }
    let delivered = std::future::poll_fn(|cx| {
        Poll::Ready(match poll.as_mut().poll(cx) {
            Poll::Ready(Ok(Some(offer))) => {
                assert_eq!(offer.task.run_key, run);
                assert_eq!(
                    (offer.task.attempt, offer.task.stamp),
                    (activity.attempt, activity.stamp)
                );
                true
            }
            Poll::Pending => false,
            other => panic!("unexpected poll outcome: {other:?}"),
        })
    })
    .await;
    let after = repo
        .list_activity_dispatch_page(
            &range,
            owner.clock.now(),
            None,
            NonZeroU32::new(64).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        before
            .candidates
            .iter()
            .map(|c| c.row.as_ref().unwrap())
            .collect::<Vec<_>>(),
        after
            .candidates
            .iter()
            .map(|c| c.row.as_ref().unwrap())
            .collect::<Vec<_>>(),
        "volatile delivery cannot consume durable intent"
    );
    assert!(owner.activity_discovery.test_gate.is_held());
    discovery_tests::stop(&owner).await;
    delivered
}

fn explore(path: Path, disconnected: bool) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    let checked = std::cell::Cell::new(0);
    let failures = std::cell::Cell::new(0);
    let mut runner = TestRunner::new_with_rng(
        Config {
            cases: 100,
            source_file: Some(file!()),
            failure_persistence: Some(Box::new(FileFailurePersistence::WithSource(
                "proptest-regressions",
            ))),
            ..Config::default()
        },
        TestRng::from_seed(RngAlgorithm::ChaCha, &[29; 32]),
    );
    // Run the entire generated batch before asking for the minimal witness.
    // Otherwise a universally failing property stops at its first generated case.
    runner
        .run(&(1u32..64), |variant| {
            checked.set(checked.get() + 1);
            if !rt.block_on(publication(path, variant, false, !disconnected, false)) {
                failures.set(failures.get() + 1);
            }
            Ok(())
        })
        .unwrap();
    let checked = checked.get();
    let failures = failures.get();
    assert!(checked >= 100);
    if failures == 0 {
        assert!(!disconnected, "disconnected control unexpectedly delivered");
        return;
    }
    let mut runner = TestRunner::new_with_rng(
        Config {
            cases: 100,
            source_file: Some(file!()),
            failure_persistence: Some(Box::new(FileFailurePersistence::WithSource(
                "proptest-regressions",
            ))),
            ..Config::default()
        },
        TestRng::from_seed(RngAlgorithm::ChaCha, &[29; 32]),
    );
    let result = runner.run(&(1u32..64), |variant| {
        if rt.block_on(publication(path, variant, false, !disconnected, false)) {
            Ok(())
        } else {
            Err(TestCaseError::fail(
                "due activity did not reach the parked queue-home poll; discovery was not driven",
            ))
        }
    });
    let Err(TestError::Fail(reason, minimal)) = result else {
        panic!("missing counterexample: {result:?}")
    };
    assert_eq!(minimal, 1, "retain the shrunk counterexample");
    if !disconnected {
        panic!(
            "{path:?}: {checked} generated cases, {failures} failures; minimal variant={minimal}: {reason}"
        );
    }
}

macro_rules! exploration {
    ($test:ident, $negative:ident, $path:ident) => {
        // Feature: activity-queue-home-delivery, Property 1: Prompt delivery with durable recovery.
        #[test]
        fn $test() {
            explore(Path::$path, false);
        }
        // Feature: activity-queue-home-delivery, Property 1: Prompt delivery with durable recovery.
        // The disconnected topology must retain its missing-delivery witness
        // when ordinary publication gains a sender.
        #[test]
        fn $negative() {
            explore(Path::$path, true);
        }
    };
}

exploration!(
    activity_queue_home_schedule,
    activity_queue_home_disconnected_schedule,
    Schedule
);
exploration!(
    activity_queue_home_failure_backoff,
    activity_queue_home_disconnected_failure_backoff,
    FailureBackoff
);
exploration!(
    activity_queue_home_failure_due,
    activity_queue_home_disconnected_failure_due,
    FailureDue
);
exploration!(
    activity_queue_home_timeout,
    activity_queue_home_disconnected_timeout,
    Timeout
);
exploration!(
    activity_queue_home_activity_unpause,
    activity_queue_home_disconnected_activity_unpause,
    ActivityUnpause
);
exploration!(
    activity_queue_home_activity_reset,
    activity_queue_home_disconnected_activity_reset,
    ActivityReset
);
exploration!(
    activity_queue_home_workflow_unpause,
    activity_queue_home_disconnected_workflow_unpause,
    WorkflowUnpause
);
exploration!(
    activity_queue_home_queue_move,
    activity_queue_home_disconnected_queue_move,
    QueueMove
);
exploration!(
    activity_queue_home_workflow_reset,
    activity_queue_home_disconnected_workflow_reset,
    WorkflowReset
);
exploration!(
    activity_queue_home_recovery,
    activity_queue_home_disconnected_recovery,
    Recovery
);

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: activity-queue-home-delivery, Property 6: Preservation outside the defect.
    #[test]
    fn activity_queue_home_local_publication(variant in 1u32..64) {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap();
        prop_assert!(rt.block_on(publication(Path::Schedule, variant, true, true, false)));
    }
    // Feature: activity-queue-home-delivery, Property 6: Preservation outside the defect.
    #[test]
    fn activity_queue_home_discovery_recovers_lost_publication(variant in 1u32..64) {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap();
        prop_assert!(rt.block_on(publication(Path::Schedule, variant, false, false, true)));
    }
    // Feature: activity-queue-home-delivery, Property 6: Preservation outside the defect.
    #[test]
    fn activity_queue_home_eager_nonpublication(variant in 1u32..64) {
        tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap().block_on(async {
            let (repo, owner) = timer_deadline_tests::fixture().await;
            let owner = owner.with_activity_eager_execution(true);
            let request = timer_deadline_tests::start(&owner, false);
            let run = request.run_key;
            let mut queue = QueueKey { namespace_id: request.namespace_id, task_queue: request.task_queue.clone(), task_kind: TaskKind::Workflow, deployment: None, build_id: None };
            owner.start_workflow(request).await.unwrap();
            let first = owner.poll_workflow_task(queue.clone(), WorkerIdentity("eager-worker".into()), std::time::Duration::from_secs(1)).await.unwrap().unwrap();
            let mut complete = completion_inline_tests::completion(&first, false);
            complete.now = owner.clock.now();
            complete.request.received_at = complete.now;
            complete.commands = vec![completion_eager_tests::schedule(&format!("eager-{variant}"), true)];
            let reply = owner.complete_workflow_task_with_result(complete).await.unwrap();
            assert_eq!(reply.activity_tasks.len(), 1);
            queue.task_queue = TaskQueueName("activities".into());
            queue.task_kind = TaskKind::Activity;
            // Completion-owned starts bypass ordinary task generation:
            // service/history/api/respondworkflowtaskcompleted/workflow_task_completed_handler.go @ v1.31.0.
            assert!(!owner.activity_broker.has_runnable_backlog(&queue).await);
            let page = repo.list_activity_dispatch_page(&ActivityDiscoveryRange { namespace_id: queue.namespace_id, queue_name: queue.task_queue }, owner.clock.now(), None, NonZeroU32::new(64).unwrap()).await.unwrap();
            assert!(page.candidates.is_empty());
            assert!(state(&repo, run).await.activities[&format!("eager-{variant}")].started_at.is_some());
            discovery_tests::stop(&owner).await;
        });
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: activity-queue-home-delivery, Property 1: finite request/reply loss and refusal preserve durable recovery across every publication path.
    #[test]
    fn activity_delivery_fault_prefix_recovery(path in 0u8..10, variant in 1u32..64, fault in 0u8..6) {
        let path = match path {
            0 => Path::Schedule, 1 => Path::FailureBackoff, 2 => Path::FailureDue, 3 => Path::Timeout,
            4 => Path::ActivityUnpause, 5 => Path::ActivityReset, 6 => Path::WorkflowUnpause,
            7 => Path::QueueMove, 8 => Path::WorkflowReset, _ => Path::Recovery,
        };
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap();
        prop_assert!(runtime.block_on(publication_with_fault(path, variant, false, true, fault == 3 || fault == 4, fault)));
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: activity-queue-home-delivery, Property 1: a fresh execution owner rebuilds due work and pushes with destination discovery paused.
    #[test]
    fn activity_restarted_owner_pushes_without_discovery(variant in 1u32..64) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap();
        prop_assert!(runtime.block_on(publication_with_fault(Path::Recovery, variant, false, true, false, 6)));
    }
}

async fn offer_fixture(
    variant: u32,
) -> (
    TokeiraRuntime<InMemoryStore>,
    crate::ActivityQueueOffer,
    ExecutionRef,
) {
    let clock: Arc<dyn Clock> = Arc::new(TestClock::new());
    let repo = Arc::new(InMemoryStore::with_shard_count(8).with_clock(clock.clone()));
    let owner = execution_owner(repo, clock.clone(), false).await;
    let mut start = timer_deadline_tests::start(&owner, false);
    start.workflow_id = WorkflowId(format!("offer-outcomes-{variant}"));
    start.run_id = RunId(Uuid::from_u128(u128::from(variant) + 1));
    start.run_key = RunKey::derive(start.namespace_id, &start.workflow_id, start.run_id);
    let execution_home = execution_home_bundle(
        start.namespace_id.0.as_bytes(),
        start.workflow_id.0.as_bytes(),
        8,
    );
    let queue = |local| {
        (0..256)
            .map(|i| QueueKey {
                namespace_id: start.namespace_id,
                task_queue: TaskQueueName(format!("offer-{variant}-{i}")),
                task_kind: TaskKind::Activity,
                deployment: None,
                build_id: None,
            })
            .find(|q| (home(q) == execution_home) == local)
            .unwrap()
    };
    let selected = queue(false);
    let workflow = QueueKey {
        task_kind: TaskKind::Workflow,
        ..queue(true)
    };
    start.task_queue = workflow.task_queue.clone();
    let execution = ExecutionRef {
        namespace_id: start.namespace_id,
        workflow_id: start.workflow_id.clone(),
        run_id: Some(start.run_id),
    };
    let run = start.run_key;
    owner.acquire_shard(execution_home).await.unwrap();
    owner.start_workflow(start).await.unwrap();
    let first = owner
        .poll_workflow_task(
            workflow,
            WorkerIdentity("worker".into()),
            std::time::Duration::ZERO,
        )
        .await
        .unwrap()
        .unwrap();
    let mut command = completion_eager_tests::schedule("activity", false);
    if let WorkflowCommand::ScheduleActivity { task_queue, .. } = &mut command {
        *task_queue = selected.task_queue.clone();
    }
    let mut completion = completion_inline_tests::completion(&first, false);
    completion.now = clock.now();
    completion.request.received_at = completion.now;
    completion.commands = vec![command];
    owner
        .complete_workflow_task_with_result(completion)
        .await
        .unwrap();
    let committed = state(owner.repo.as_ref(), run).await;
    let offer = crate::ActivityQueueOffer::from_state(
        &committed,
        &committed.activities["activity"],
        execution_home,
        clock.now(),
    )
    .unwrap();
    (owner, offer, execution)
}

async fn canonical_rows(
    owner: &TokeiraRuntime<InMemoryStore>,
    offer: &crate::ActivityQueueOffer,
) -> Vec<tokeira_storage::ActivityDispatchRow> {
    owner
        .repo
        .list_activity_dispatch_page(
            &ActivityDiscoveryRange {
                namespace_id: offer.task.queue.namespace_id,
                queue_name: offer.task.queue.task_queue.clone(),
            },
            owner.clock.now(),
            None,
            NonZeroU32::new(64).unwrap(),
        )
        .await
        .unwrap()
        .candidates
        .into_iter()
        .map(|candidate| candidate.row.unwrap())
        .collect()
}

#[derive(Debug)]
struct FailureReceipt {
    error: crate::ActivityOfferError,
    observed: tokio::sync::Notify,
}
#[async_trait::async_trait]
impl crate::RemoteActivityOfferSink for FailureReceipt {
    async fn offer(
        &self,
        _: &crate::ActivityQueueOffer,
        _: ShardId,
        _: bool,
    ) -> std::result::Result<crate::ActivityOfferOutcome, crate::ActivityOfferError> {
        self.observed.notify_one();
        Err(self.error)
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: activity-queue-home-delivery, Property 1: every offer-only outcome and sender error leaves canonical dispatch rows byte-for-byte equivalent.
    #[test]
    fn activity_offer_outcomes_preserve_canonical_rows(variant in 1u32..64) {
        tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap().block_on(async {
            let (owner, offer, _) = offer_fixture(variant).await;
            let before = canonical_rows(&owner, &offer).await;
            prop_assert_eq!(before.len(), 1);
            for source in [crate::workflow_offers::OfferSource::Local, crate::workflow_offers::OfferSource::Pushed, crate::workflow_offers::OfferSource::Discovered] {
                for expected in [crate::ActivityOfferOutcome::Admitted, crate::ActivityOfferOutcome::Known, crate::ActivityOfferOutcome::Full, crate::ActivityOfferOutcome::Expired, crate::ActivityOfferOutcome::NotDue, crate::ActivityOfferOutcome::NotHome] {
                    let (broker, homes) = destination(&offer.task.queue);
                    let acquisition = homes.local_home(&offer.task.queue).unwrap();
                    let mut candidate = offer.clone();
                    let mut reservation = None;
                    match expected {
                        crate::ActivityOfferOutcome::Known => {
                            prop_assert_eq!(broker.admit_activity_offer(candidate.clone(), Some(&acquisition), source, owner.clock.now(), None).await, crate::ActivityOfferOutcome::Admitted);
                            prop_assert_eq!(canonical_rows(&owner, &offer).await, before.clone());
                        }
                        crate::ActivityOfferOutcome::Full => { reservation = Some(broker.reserve_offers(&acquisition, &offer.task.queue, 8192).await); }
                        crate::ActivityOfferOutcome::Expired => { candidate.expires_at = owner.clock.now(); }
                        crate::ActivityOfferOutcome::NotDue => { candidate.dispatch_at = owner.clock.now() + Duration::seconds(1); }
                        crate::ActivityOfferOutcome::NotHome => acquisition.cancel.cancel(),
                        crate::ActivityOfferOutcome::Admitted => {}
                    }
                    prop_assert_eq!(broker.admit_activity_offer(candidate, Some(&acquisition), source, owner.clock.now(), None).await, expected);
                    prop_assert_eq!(canonical_rows(&owner, &offer).await, before.clone());
                    drop(reservation);
                }
            }
            for error in [crate::ActivityOfferError::Routing, crate::ActivityOfferError::Authentication, crate::ActivityOfferError::Capacity, crate::ActivityOfferError::Transport, crate::ActivityOfferError::Deadline, crate::ActivityOfferError::Invalid, crate::ActivityOfferError::Unimplemented] {
                let shutdown = crate::RuntimeShutdownHandle::new();
                let homes = Arc::new(PlacementQueueHomes { owner: owner.shard_owner.clone(), counts: RwLock::new((8, 8)) });
                let delivery = crate::activity_delivery::ActivityDelivery::new(InMemoryActivityBroker::with_discovery(homes.clone()), homes, shutdown.clone(), owner.clock.clone());
                let sink = Arc::new(FailureReceipt { error, observed: tokio::sync::Notify::new() });
                delivery.install_sender(sink.clone());
                let acquisition = owner.shard_owner.read().unwrap().acquisition(offer.execution_home).unwrap();
                delivery.publish(offer.clone(), acquisition, crate::activity_delivery::ActivityDeliverySource::Publication).await;
                sink.observed.notified().await;
                shutdown.begin_shutdown(); shutdown.close_registration(); shutdown.wait(std::time::Instant::now() + std::time::Duration::from_secs(1)).await.unwrap();
                prop_assert_eq!(canonical_rows(&owner, &offer).await, before.clone(), "sender error {:?}", error);
            }
            discovery_tests::stop(&owner).await;
            Ok(())
        })?;
    }

    // Feature: activity-queue-home-delivery, Property 1: an actual signal refresh preserves the completed delivery descriptor for an unchanged incarnation.
    #[test]
    fn activity_signal_does_not_rearm_completed_delivery(variant in 1u32..64) {
        tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap().block_on(async {
            let (owner, offer, execution) = offer_fixture(variant).await;
            let (broker, homes) = destination(&offer.task.queue);
            let (acknowledgements, mut received) = tokio::sync::mpsc::unbounded_channel();
            owner.set_activity_offer_sink(Arc::new(ActivitySink { fault: std::sync::atomic::AtomicU8::new(0), broker, homes, clock: owner.clock.clone(), acknowledgements }));
            let deps = owner.activity_retry_deps();
            scan_activity_eligibility_once(&deps, offer.execution_home, owner.clock.now()).await;
            received.recv().await.unwrap();
            let maximum = std::time::Duration::from_secs(60);
            prop_assert_eq!(owner.activity_tracking.eligibility.deadline_delay(owner.clock.now(), maximum), maximum);
            let before = canonical_rows(&owner, &offer).await;
            let revision = state(owner.repo.as_ref(), offer.task.run_key).await.last_event_id;
            owner.signal_workflow(execution, SignalRequest { signal_name: "unrelated".into(), input: Payloads::default(), header: None, links: Vec::new(), request: request_context(owner.clock.now()), now: owner.clock.now() }).await.unwrap();
            prop_assert!(state(owner.repo.as_ref(), offer.task.run_key).await.last_event_id > revision);
            owner.activity_tracking.eligibility.begin_full_pass();
            prop_assert_eq!(owner.activity_tracking.eligibility.deadline_delay(owner.clock.now(), maximum), maximum, "signal must not reinstall a due obligation");
            scan_activity_eligibility_once(&deps, offer.execution_home, owner.clock.now()).await;
            prop_assert!(received.try_recv().is_err());
            prop_assert_eq!(canonical_rows(&owner, &offer).await, before);
            discovery_tests::stop(&owner).await;
            Ok(())
        })?;
    }
}
