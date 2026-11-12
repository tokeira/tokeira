//! A lane routes the dispatch of a transition it has just committed from that
//! transition's state: the publish reads no run back from storage, and it
//! routes each task to the queue a publish that reloads the run would.

use super::*;
use crate::{
    activity_timeout::ActivityTrackingState,
    broker::{InMemoryActivityBroker, InMemoryBroker},
    deployment_registry::{DeploymentRegistry, RegisterPolledDeployment, SetCurrent},
    fairness::DeliveryMetrics,
    nexus::{
        CompletionCallbackTrackingState, NexusCompletionRuntimeConfig, NexusEndpointRegistry,
        NexusTaskBroker, NexusTimeoutTrackingState, NoopNexusCompletionClient, NoopNexusHttpClient,
    },
    publisher::RuntimeDispatchPublisher,
    worker_registry::WorkerRegistry,
};
use metrics_util::debugging::DebuggingRecorder;
use tokeira_kernel::VersioningBehavior;
use tokeira_storage::{
    BuildId as StoredBuildId, DeploymentName, DeploymentTaskQueueType, InMemoryStore,
};
use tokeira_types::{BuildId, DeploymentId};

const DEPLOYMENT: &str = "deployment";
const BUILD: &str = "v1";
const ACTIVITY_QUEUE: &str = "activity-q";
const STICKY_QUEUE: &str = "sticky-q";
const STICKY_WORKER: &str = "worker-v1";

/// Records how many run reads each lane publish made.
#[derive(Clone)]
struct ReadCountingPublisher {
    inner: RuntimeDispatchPublisher<MockRepo>,
    repo: MockRepo,
    committed_publish_reads: Arc<Mutex<Vec<usize>>>,
}

#[async_trait]
impl DispatchPublisher for ReadCountingPublisher {
    async fn publish(
        &self,
        run_key: RunKey,
        execution_home: ShardId,
        ops: &[DispatchOp],
    ) -> Result<()> {
        self.inner.publish(run_key, execution_home, ops).await
    }

    async fn publish_committed(
        &self,
        committed: &WorkflowState,
        execution_home: ShardId,
        ops: &[DispatchOp],
    ) -> Result<()> {
        let before = self.repo.snapshot().await.0;
        let result = self
            .inner
            .publish_committed(committed, execution_home, ops)
            .await;
        let reads = self.repo.snapshot().await.0 - before;
        self.committed_publish_reads.lock().unwrap().push(reads);
        result
    }

    async fn submit_to_run(&self, run_key: RunKey, command: Command) -> Result<CommitResult> {
        self.inner.submit_to_run(run_key, command).await
    }
}

struct Brokers {
    workflow: InMemoryBroker,
    activity: InMemoryActivityBroker,
}

fn publisher(
    repo: &MockRepo,
    registry: &Arc<RwLock<Option<DeploymentRegistry>>>,
) -> (RuntimeDispatchPublisher<MockRepo>, Brokers) {
    publisher_with_workflow_broker(repo, registry, InMemoryBroker::default())
}

fn publisher_with_workflow_broker(
    repo: &MockRepo,
    registry: &Arc<RwLock<Option<DeploymentRegistry>>>,
    workflow: InMemoryBroker,
) -> (RuntimeDispatchPublisher<MockRepo>, Brokers) {
    let brokers = Brokers {
        workflow,
        activity: InMemoryActivityBroker::default(),
    };
    let publisher = RuntimeDispatchPublisher::new(
        brokers.workflow.clone(),
        brokers.activity.clone(),
        Arc::new(repo.clone()),
        Arc::new(Mutex::new(Vec::new())),
        1,
        Arc::new(NoopNexusHttpClient),
        Arc::new(NoopNexusCompletionClient),
        NexusCompletionRuntimeConfig::default(),
        NexusEndpointRegistry::default(),
        NexusTaskBroker::default(),
        NexusTimeoutTrackingState::default(),
        CompletionCallbackTrackingState::default(),
        ActivityTrackingState::default(),
        DeliveryMetrics::new(),
    )
    .with_worker_deployment_registry(registry.clone());
    (publisher, brokers)
}

/// An auto-upgrade run on `DEPLOYMENT`/`BUILD`, whose workflow and activity
/// queues both route to that Current version.
async fn versioned_run() -> (WorkflowState, Arc<RwLock<Option<DeploymentRegistry>>>) {
    let mut state = sample_state(RunKey::new());
    state.versioning_info = Some(WorkflowVersioningInfo {
        behavior: VersioningBehavior::AutoUpgrade,
        deployment_version: Some(WorkerDeploymentVersionRef {
            deployment_name: DEPLOYMENT.to_string(),
            build_id: BUILD.to_string(),
        }),
        revision_number: 1,
        ..WorkflowVersioningInfo::default()
    });
    state.worker_deployment_name = Some(DEPLOYMENT.to_string());

    let registry = DeploymentRegistry::new(
        Arc::new(InMemoryStore::default()),
        WorkerRegistry::default(),
    );
    for (task_queue, task_queue_type) in [
        (
            state.task_queue.0.clone(),
            DeploymentTaskQueueType::Workflow,
        ),
        (
            ACTIVITY_QUEUE.to_string(),
            DeploymentTaskQueueType::Activity,
        ),
    ] {
        registry
            .register_polled_deployment(RegisterPolledDeployment {
                namespace_id: state.namespace_id,
                deployment_name: DeploymentName(DEPLOYMENT.to_string()),
                build_id: StoredBuildId(BUILD.to_string()),
                task_queue,
                task_queue_type,
                identity: STICKY_WORKER.to_string(),
            })
            .await
            .expect("register deployment version");
    }
    registry
        .set_current_version(SetCurrent {
            namespace_id: state.namespace_id,
            deployment_name: DeploymentName(DEPLOYMENT.to_string()),
            build_id: Some(StoredBuildId(BUILD.to_string())),
            conflict_token: None,
            identity: "operator".to_string(),
            allow_no_pollers: false,
            ignore_missing_task_queues: true,
        })
        .await
        .expect("set deployment Current");
    (state, Arc::new(RwLock::new(Some(registry))))
}

fn routed(queue: QueueKey) -> QueueKey {
    QueueKey {
        deployment: Some(DeploymentId(DEPLOYMENT.to_string())),
        build_id: Some(BuildId(BUILD.to_string())),
        ..queue
    }
}

async fn observe_sticky_liveness(brokers: &Brokers, sticky: &QueueKey) {
    assert!(
        brokers
            .workflow
            .poll_workflow_task(
                sticky,
                &WorkerIdentity(STICKY_WORKER.to_string()),
                StdDuration::ZERO,
            )
            .await
            .expect("sticky liveness observation")
            .is_none()
    );
}

/// Whether a publish left runnable work on the queue it should route to.
async fn runnable(brokers: &Brokers, queue: &QueueKey) -> bool {
    match queue.task_kind {
        TaskKind::Activity => brokers.activity.has_runnable_backlog(queue).await,
        _ => brokers.workflow.has_runnable_backlog(queue).await,
    }
}

/// Commits one transition through a lane whose kernel emits `ops`, then
/// publishes the same ops through the reloading path. Returns the run reads of
/// the lane's publish, the reads of the reloading publish, and whether each
/// left runnable work on `expected`.
async fn lane_publish_then_reload(
    state: WorkflowState,
    registry: Arc<RwLock<Option<DeploymentRegistry>>>,
    ops: SmallVec<[DispatchOp; 4]>,
    expected: &QueueKey,
    sticky: Option<&QueueKey>,
) -> (Vec<usize>, usize, bool, bool) {
    let run_key = state.run_key;
    let repo = MockRepo::new(LoadedRun::Existing(state), Vec::new());
    let (inner, lane_brokers) = publisher(&repo, &registry);
    let counting = ReadCountingPublisher {
        inner,
        repo: repo.clone(),
        committed_publish_reads: Arc::new(Mutex::new(Vec::new())),
    };
    if let Some(sticky) = sticky {
        observe_sticky_liveness(&lane_brokers, sticky).await;
    }
    let lane = spawn_lane(
        MockKernel::new(ops.clone()),
        repo.clone(),
        counting.clone(),
        test_shard_owner(),
        ActivityTrackingState::default(),
        crate::timeout::WorkflowTimeoutTrackingState::default(),
        crate::wft_timeout::WftTimeoutTrackingState::default(),
        NexusTimeoutTrackingState::default(),
        UpdateRegistry::new(),
        LaneConfig::default(),
    );
    assert!(matches!(
        lane.submit(run_key, sample_command("commit"))
            .await
            .unwrap(),
        CommitResult::Applied { .. }
    ));
    let committed_reads = counting.committed_publish_reads.lock().unwrap().clone();

    // The same ops over the stored copy of the committed state, read back.
    let (reloading, reload_brokers) = publisher(&repo, &registry);
    if let Some(sticky) = sticky {
        observe_sticky_liveness(&reload_brokers, sticky).await;
    }
    let before = repo.snapshot().await.0;
    reloading
        .publish(run_key, ShardId(0), &ops)
        .await
        .expect("reloading publish");
    let reload_reads = repo.snapshot().await.0 - before;

    (
        committed_reads,
        reload_reads,
        runnable(&lane_brokers, expected).await,
        runnable(&reload_brokers, expected).await,
    )
}

#[tokio::test]
async fn lane_publish_routes_a_workflow_task_from_committed_state() {
    let (state, registry) = versioned_run().await;
    let queue = QueueKey {
        namespace_id: state.namespace_id,
        task_queue: state.task_queue.clone(),
        task_kind: TaskKind::Workflow,
        deployment: None,
        build_id: None,
    };
    let ops = smallvec![DispatchOp::EnqueueWorkflowTask {
        queue: queue.clone(),
        logical_seq: LogicalTaskSeq::ONE,
        sticky_preferred: None,
        normal_task_queue: None,
        speculative: false,
        priority: None,
    }];

    let (committed_reads, reload_reads, lane_runnable, reload_runnable) =
        lane_publish_then_reload(state, registry, ops, &routed(queue), None).await;

    assert_eq!(committed_reads, vec![0]);
    assert_eq!(reload_reads, 1);
    assert_eq!((lane_runnable, reload_runnable), (true, true));
}

#[tokio::test]
async fn lane_publish_routes_a_sticky_workflow_task_from_committed_state() {
    let (state, registry) = versioned_run().await;
    let sticky = QueueKey {
        namespace_id: state.namespace_id,
        task_queue: TaskQueueName(STICKY_QUEUE.to_string()),
        task_kind: TaskKind::Workflow,
        deployment: None,
        build_id: None,
    };
    let ops = smallvec![DispatchOp::EnqueueWorkflowTask {
        queue: sticky.clone(),
        logical_seq: LogicalTaskSeq::ONE,
        sticky_preferred: Some(WorkerIdentity(STICKY_WORKER.to_string())),
        normal_task_queue: Some(state.task_queue.clone()),
        speculative: false,
        priority: None,
    }];
    let hydrated_sticky = routed(sticky);

    let (committed_reads, reload_reads, lane_runnable, reload_runnable) = lane_publish_then_reload(
        state,
        registry,
        ops,
        &hydrated_sticky,
        Some(&hydrated_sticky),
    )
    .await;

    assert_eq!(committed_reads, vec![0]);
    // Sticky hydration and the normal queue's routing each reload the run.
    assert_eq!(reload_reads, 2);
    assert_eq!((lane_runnable, reload_runnable), (true, true));
}

#[tokio::test]
async fn lane_publish_routes_an_activity_task_from_committed_state() {
    let (state, registry) = versioned_run().await;
    let queue = QueueKey {
        namespace_id: state.namespace_id,
        task_queue: TaskQueueName(ACTIVITY_QUEUE.to_string()),
        task_kind: TaskKind::Activity,
        deployment: None,
        build_id: None,
    };
    let ops = smallvec![DispatchOp::EnqueueActivityTask {
        queue: queue.clone(),
        activity_id: "activity-1".to_string(),
        schedule_event_id: 5,
        attempt: 1,
        dispatch_revision: 0,
        stamp: 0,
        dispatch_at: OffsetDateTime::UNIX_EPOCH,
        schedule_to_close_timeout: None,
        schedule_to_start_timeout: None,
        start_to_close_timeout: None,
        heartbeat_timeout: None,
        priority: None,
    }];

    let (committed_reads, reload_reads, lane_runnable, reload_runnable) =
        lane_publish_then_reload(state, registry, ops, &routed(queue), None).await;

    assert_eq!(committed_reads, vec![0]);
    assert_eq!(reload_reads, 1);
    assert_eq!((lane_runnable, reload_runnable), (true, true));
}

#[derive(Debug)]
struct PublicationClock(OffsetDateTime);
impl Clock for PublicationClock {
    fn now(&self) -> OffsetDateTime {
        self.0
    }
}

#[derive(Clone)]
struct PausedOfferPublisher {
    inner: RuntimeDispatchPublisher<MockRepo>,
    release: Arc<Notify>,
}
#[async_trait]
impl DispatchPublisher for PausedOfferPublisher {
    async fn publish(&self, run: RunKey, home: ShardId, ops: &[DispatchOp]) -> Result<()> {
        self.inner.publish(run, home, ops).await
    }
    async fn publish_committed(
        &self,
        state: &WorkflowState,
        home: ShardId,
        ops: &[DispatchOp],
    ) -> Result<()> {
        self.inner.publish_committed(state, home, ops).await?;
        self.release.notified().await;
        Ok(())
    }
    async fn submit_to_run(&self, run: RunKey, command: Command) -> Result<CommitResult> {
        self.inner.submit_to_run(run, command).await
    }
}
#[derive(Debug)]
struct AdmissionReceiptSink {
    receiver: Arc<crate::workflow_delivery::WorkflowDelivery<MockRepo>>,
    replies: tokio::sync::mpsc::UnboundedSender<crate::WorkflowOfferOutcome>,
}
#[async_trait]
impl crate::RemoteWorkflowOfferSink for AdmissionReceiptSink {
    async fn offer(
        &self,
        offer: &crate::WorkflowOffer,
        destination: ShardId,
        _: bool,
    ) -> Result<crate::WorkflowOfferOutcome, crate::WorkflowOfferError> {
        let outcome = self
            .receiver
            .receive(
                offer,
                destination,
                crate::workflow_offers::OfferSource::Pushed,
            )
            .await?;
        self.replies.send(outcome).unwrap();
        Ok(outcome)
    }
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: queue-home-delivery, Property 7: a real lane stays paused in post-commit publication until its remote receipt arrives.
    #[test]
    fn property_7_publishing_lane_paused_until_receipt(index in 0u32..100) {
        tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap().block_on(async {
            let mut state = sample_state(RunKey::new());
            state.task_queue = (index..index+100).map(|i| TaskQueueName(format!("receipt-{i}"))).find(|name| {
                tokeira_types::bundle_for_partition(tokeira_types::workflow_queue_partition(state.namespace_id, &name.0, 8), 8) != ShardId(0)
            }).unwrap();
            let queue = QueueKey { namespace_id: state.namespace_id, task_queue: state.task_queue.clone(), task_kind: TaskKind::Workflow, deployment: None, build_id: None };
            let repo = MockRepo::new(LoadedRun::Existing(state.clone()), Vec::new());
            let registry = Arc::new(RwLock::new(None));
            let (inner, _) = publisher(&repo, &registry);
            let owner = test_shard_owner();
            let source_homes = Arc::new(crate::discovery::PlacementQueueHomes { owner: owner.clone(), counts: RwLock::new((8, 8)) });
            let source = crate::workflow_delivery::WorkflowDelivery::new(Arc::new(repo.clone()), registry.clone(), InMemoryBroker::with_discovery(source_homes.clone()), source_homes, crate::RuntimeShutdownHandle::new(), system_clock());
            let mut target_owner = ShardOwner::new(8);
            for id in 0..8 { target_owner.record_acquired(ShardId(id), ShardEpoch::ZERO); target_owner.mark_active(ShardId(id)); }
            let target_homes = Arc::new(crate::discovery::PlacementQueueHomes { owner: Arc::new(RwLock::new(target_owner)), counts: RwLock::new((8, 8)) });
            let receiver = crate::workflow_delivery::WorkflowDelivery::new(Arc::new(repo.clone()), registry, InMemoryBroker::with_discovery(target_homes.clone()), target_homes, crate::RuntimeShutdownHandle::new(), system_clock());
            let (replies, mut acknowledgements) = tokio::sync::mpsc::unbounded_channel();
            source.install_sender(Arc::new(AdmissionReceiptSink { receiver, replies }));
            let release = Arc::new(Notify::new());
            let publisher = PausedOfferPublisher { inner: inner.with_workflow_delivery(source), release: release.clone() };
            let lane = spawn_lane(MockKernel::new(smallvec![DispatchOp::EnqueueWorkflowTask { queue, logical_seq: LogicalTaskSeq::ONE, sticky_preferred: None, normal_task_queue: None, speculative: false, priority: None }]), repo, publisher, owner,
                ActivityTrackingState::default(), crate::timeout::WorkflowTimeoutTrackingState::default(), crate::wft_timeout::WftTimeoutTrackingState::default(), NexusTimeoutTrackingState::default(), UpdateRegistry::new(), LaneConfig::default());
            let submitted = tokio::spawn(async move { lane.submit(state.run_key, sample_command("publish")).await });
            let acknowledged = tokio::time::timeout(StdDuration::from_secs(1), acknowledgements.recv()).await.unwrap().unwrap();
            prop_assert_eq!(acknowledged, crate::WorkflowOfferOutcome::Admitted);
            prop_assert!(!submitted.is_finished());
            release.notify_one();
            prop_assert!(matches!(submitted.await.unwrap().unwrap(), CommitResult::Applied { .. }), "lane commits after receipt");
            Ok(())
        })?;
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: queue-home-delivery, Property 4: committed local fallback needs no run reload, even when every load fails.
    #[test]
    fn property_4_local_fallback_with_failed_run_loads(index in 0u32..100) {
        let recorder = DebuggingRecorder::new();
        metrics::with_local_recorder(&recorder, || tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap().block_on(async {
            let clock: Arc<dyn Clock> = Arc::new(PublicationClock(OffsetDateTime::UNIX_EPOCH + time::Duration::days(20_000)));
            let (base, registry) = versioned_run().await;
            let normal = QueueKey { namespace_id: base.namespace_id, task_queue: base.task_queue, task_kind: TaskKind::Workflow, deployment: None, build_id: None };
            let mut state = crate::discovery::tests::transition(&normal, index as usize).next_state;
            state.versioning_info = base.versioning_info;
            state.worker_deployment_name = base.worker_deployment_name;
            let sticky = QueueKey { task_queue: TaskQueueName(format!("sticky-{index}")), ..normal.clone() };
            let deadline = clock.now() + time::Duration::seconds(30);
            state.pending_workflow_task.as_mut().unwrap().schedule_to_start_deadline = Some(deadline);
            state.sticky = Some(tokeira_types::StickyAffinity { sticky_queue: sticky.task_queue.clone(), worker_identity: WorkerIdentity(STICKY_WORKER.into()), schedule_to_start_timeout: time::Duration::seconds(30) });
            let repo = MockRepo::new(LoadedRun::Existing(state.clone()), Vec::new());
            repo.state.lock().await.fail_loads = true;
            let homes = Arc::new(crate::discovery::PlacementQueueHomes { owner: test_shard_owner(), counts: RwLock::new((1, 1)) });
            let (publisher, brokers) = publisher_with_workflow_broker(&repo, &registry, InMemoryBroker::with_discovery(homes.clone()).with_clock(clock.clone()));
            let delivery = crate::workflow_delivery::WorkflowDelivery::new(Arc::new(repo.clone()), registry, brokers.workflow.clone(), homes, crate::RuntimeShutdownHandle::new(), clock.clone());
            let publisher = publisher.with_workflow_delivery(delivery).with_clock(clock);
            let seq = state.pending_workflow_task.as_ref().unwrap().logical_seq;
            publisher.publish_committed(&state, ShardId(0), &[DispatchOp::EnqueueWorkflowTask { queue: sticky, logical_seq: seq, sticky_preferred: Some(WorkerIdentity(STICKY_WORKER.into())), normal_task_queue: Some(normal.task_queue.clone()), speculative: false, priority: None }]).await.unwrap();
            prop_assert!(brokers.workflow.has_runnable_backlog(&routed(normal.clone())).await, "normal fallback must be retained; metrics={:?}", recorder.snapshotter().snapshot().into_vec());
            let (task, _) = brokers.workflow.try_claim_workflow_task(&routed(normal), state.run_key).await.unwrap();
            prop_assert_eq!(task.sticky_deadline, Some(deadline));
            prop_assert_eq!(repo.snapshot().await.0, 0);
            Ok(())
        }))?;
    }
}
