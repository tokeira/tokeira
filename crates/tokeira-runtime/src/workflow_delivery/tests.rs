//! Generated receiver and publication schedules over real volatile admission and
//! committed memory-store intent. No discovery loop or start lane drives receipts.

use super::*;
use crate::{
    WorkerRegistry,
    deployment_registry::{RegisterPolledDeployment, SetCurrent},
    shard::ShardOwner,
};
use proptest::prelude::*;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};
use tokeira_storage::{
    ConflictToken, DeploymentCasResult, DeploymentKey, DeploymentName, DeploymentTaskQueueType,
    InMemoryStore, StoredWorkerDeployment, WorkerDeploymentRepository, WriteFence,
};
use tokeira_types::{
    NamespaceId, ShardEpoch, StickyAffinity, TaskKind, TaskQueueName, WorkerIdentity,
};
use tokio::{
    sync::mpsc,
    time::{Duration, Instant},
};

#[derive(Debug)]
struct TestClock {
    wall: time::OffsetDateTime,
    start: Instant,
}
impl Clock for TestClock {
    fn now(&self) -> time::OffsetDateTime {
        self.wall + time::Duration::try_from(self.start.elapsed()).unwrap()
    }
}
pub(super) fn clock() -> Arc<dyn Clock> {
    Arc::new(TestClock {
        wall: time::OffsetDateTime::UNIX_EPOCH + time::Duration::days(20_000),
        start: Instant::now(),
    })
}
pub(super) fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap()
}
pub(super) fn queue(index: u32) -> QueueKey {
    QueueKey {
        namespace_id: NamespaceId(uuid::Uuid::from_u128(17)),
        task_queue: TaskQueueName(format!("delivery-{index}")),
        task_kind: TaskKind::Workflow,
        deployment: None,
        build_id: None,
    }
}
pub(super) fn home(queue: &QueueKey) -> ShardId {
    tokeira_types::bundle_for_partition(
        tokeira_types::workflow_queue_partition(queue.namespace_id, &queue.task_queue.0, 8),
        8,
    )
}
pub(super) fn node<R: RunRepository + 'static>(
    repo: Arc<R>,
    ids: &[ShardId],
    clock: Arc<dyn Clock>,
) -> Arc<WorkflowDelivery<R>> {
    let mut owner = ShardOwner::new(8);
    for id in ids {
        owner.record_acquired(*id, ShardEpoch::ZERO);
        owner.mark_active(*id);
    }
    let homes = Arc::new(PlacementQueueHomes {
        owner: Arc::new(RwLock::new(owner)),
        counts: RwLock::new((8, 8)),
    });
    WorkflowDelivery::new(
        repo,
        Arc::new(RwLock::new(None)),
        InMemoryBroker::with_discovery(homes.clone()).with_clock(clock.clone()),
        homes,
        RuntimeShutdownHandle::new(),
        clock,
    )
}
pub(super) async fn stored(
    repo: &InMemoryStore,
    normal: &QueueKey,
    sticky: Option<&QueueKey>,
    index: u32,
    clock: &dyn Clock,
) -> WorkflowOffer {
    let mut item = crate::discovery::tests::transition(normal, index as usize);
    let state = &mut item.next_state;
    state.pending_workflow_task.as_mut().unwrap().scheduled_at = clock.now();
    if let Some(sticky) = sticky {
        state.sticky = Some(StickyAffinity {
            sticky_queue: sticky.task_queue.clone(),
            worker_identity: WorkerIdentity("worker".into()),
            schedule_to_start_timeout: time::Duration::seconds(30),
        });
        state
            .pending_workflow_task
            .as_mut()
            .unwrap()
            .schedule_to_start_deadline = Some(clock.now() + time::Duration::seconds(30));
    }
    let execution = tokeira_types::execution_home_bundle(
        state.namespace_id.0.as_bytes(),
        state.workflow_id.0.as_bytes(),
        8,
    );
    let offer = WorkflowOffer::from_state(state, execution)
        .unwrap()
        .unwrap();
    repo.commit_transition(state.run_key, item, WriteFence::Unfenced)
        .await
        .unwrap();
    offer
}
pub(super) async fn recent<R: RunRepository + 'static>(
    delivery: &WorkflowDelivery<R>,
    offer: &WorkflowOffer,
) {
    let origin = offer.hint.sticky_origin.as_ref().unwrap();
    delivery
        .broker
        .poll_workflow_task(&origin.sticky_queue, &origin.worker, Duration::ZERO)
        .await
        .unwrap();
}
#[derive(Debug)]
struct RegistryStore {
    repo: Arc<InMemoryStore>,
    fail: AtomicBool,
}
#[async_trait]
impl WorkerDeploymentRepository for RegistryStore {
    async fn load_deployment(&self, key: &DeploymentKey) -> Result<Option<StoredWorkerDeployment>> {
        if self.fail.load(Ordering::SeqCst) {
            anyhow::bail!("injected routing failure");
        }
        self.repo.load_deployment(key).await
    }
    async fn list_all_for_namespace(
        &self,
        namespace: NamespaceId,
    ) -> Result<Vec<StoredWorkerDeployment>> {
        if self.fail.load(Ordering::SeqCst) {
            anyhow::bail!("injected routing failure");
        }
        self.repo.list_all_for_namespace(namespace).await
    }
    async fn put_deployment(
        &self,
        record: StoredWorkerDeployment,
        expected: Option<ConflictToken>,
    ) -> Result<DeploymentCasResult> {
        self.repo.put_deployment(record, expected).await
    }
    async fn delete_deployment(
        &self,
        key: &DeploymentKey,
        expected: ConflictToken,
    ) -> Result<DeploymentCasResult> {
        self.repo.delete_deployment(key, expected).await
    }
    async fn list_deployments(
        &self,
        namespace: NamespaceId,
        after: Option<&DeploymentName>,
        limit: usize,
    ) -> Result<Vec<StoredWorkerDeployment>> {
        self.repo.list_deployments(namespace, after, limit).await
    }
}
async fn registry(
    node: &WorkflowDelivery<InMemoryStore>,
    normal: &QueueKey,
    changed: bool,
) -> Arc<RegistryStore> {
    let store = Arc::new(RegistryStore {
        repo: node.repo.clone(),
        fail: AtomicBool::new(false),
    });
    let registry = DeploymentRegistry::with_clock(
        store.clone(),
        WorkerRegistry::default(),
        node.clock.clone(),
    );
    for build in ["v1", "v2"] {
        registry
            .register_polled_deployment(RegisterPolledDeployment {
                namespace_id: normal.namespace_id,
                deployment_name: DeploymentName("deploy".into()),
                build_id: tokeira_storage::BuildId(build.into()),
                task_queue: normal.task_queue.0.clone(),
                task_queue_type: DeploymentTaskQueueType::Workflow,
                identity: "worker".into(),
            })
            .await
            .unwrap();
    }
    registry
        .set_current_version(SetCurrent {
            namespace_id: normal.namespace_id,
            deployment_name: DeploymentName("deploy".into()),
            build_id: Some(tokeira_storage::BuildId(
                if changed { "v2" } else { "v1" }.into(),
            )),
            conflict_token: None,
            identity: "operator".into(),
            allow_no_pollers: false,
            ignore_missing_task_queues: true,
        })
        .await
        .unwrap();
    *node.registry.write().unwrap() = Some(registry);
    store
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: queue-home-delivery, Property 4: semantic causes, their controls and transient errors are distinct.
    #[test]
    fn property_4_sticky_conditions_and_controls(index in 0u32..100, pinned in any::<bool>(), assigned in any::<bool>(), shutdown in any::<bool>()) {
        runtime().block_on(async {
            for changed in [false, true] { for polled in [false, true] {
                let repo = Arc::new(InMemoryStore::with_shard_count(8)); let clock = clock();
                let normal = queue(index); let sticky = queue(index + 100);
                let mut offer = stored(&repo, &normal, Some(&sticky), index, clock.as_ref()).await;
                offer.directive.behavior = if pinned { VersioningBehavior::Pinned } else { VersioningBehavior::AutoUpgrade };
                offer.directive.deployment = Some(WorkerDeploymentVersionRef { deployment_name: "deploy".into(), build_id: "v1".into() });
                offer.directive.assigned_build_id = assigned.then(|| BuildId("assigned".into()));
                let node = node(repo.clone(), &[home(&sticky)], clock);
                let store = registry(&node, &normal, changed).await;
                if polled { recent(&node, &offer).await; }
                if shutdown { node.broker.deny_worker(sticky.namespace_id, sticky.task_queue.clone(), WorkerIdentity("worker".into())).await; }
                let expected = if !polled || shutdown { WorkflowOfferOutcome::StickyUnavailable(StickyUnavailableReason::NoRecentPoller) }
                    else if changed && !pinned && !assigned { WorkflowOfferOutcome::StickyUnavailable(StickyUnavailableReason::DeploymentChanged) } else { WorkflowOfferOutcome::Admitted };
                prop_assert_eq!(node.receive(&offer, home(&sticky), OfferSource::Pushed).await.unwrap(), expected);
                store.fail.store(true, Ordering::SeqCst);
                prop_assert_eq!(node.receive(&offer, home(&sticky), OfferSource::Pushed).await, if !polled || shutdown { Ok(WorkflowOfferOutcome::StickyUnavailable(StickyUnavailableReason::NoRecentPoller)) } else { Err(WorkflowOfferError::Routing) });
                store.fail.store(false, Ordering::SeqCst);
                node.homes.owner.write().unwrap().remove(home(&sticky));
                prop_assert_eq!(node.receive(&offer, home(&sticky), OfferSource::Pushed).await.unwrap(), WorkflowOfferOutcome::NotHome);
            }}
            Ok(())
        })?;
    }

    // Feature: queue-home-delivery, Property 4: arrival order cannot evict a live sticky revision, but a local decision can.
    #[test]
    fn property_4_same_home_fallback_and_absolute_deadline(index in 0u32..100, copies in 1usize..8, start_before_deadline in any::<bool>()) {
        runtime().block_on(async {
            let repo = Arc::new(InMemoryStore::with_shard_count(8)); let clock = clock();
            let normal = queue(index); let sticky = (index + 1..index + 100).map(queue).find(|q| home(q) == home(&normal)).unwrap();
            let offer = stored(&repo, &normal, Some(&sticky), index, clock.as_ref()).await;
            let node = node(repo.clone(), &[home(&normal)], clock.clone());
            recent(&node, &offer).await;
            prop_assert_eq!(node.receive(&offer, home(&sticky), OfferSource::Pushed).await.unwrap(), WorkflowOfferOutcome::Admitted);
            let fallback = node.fallback(offer.clone(), PublicationSource::Publication).await.unwrap().unwrap();
            prop_assert_eq!(node.receive(&fallback, home(&normal), OfferSource::Pushed).await.unwrap(), WorkflowOfferOutcome::Known);
            node.broker.deny_worker(sticky.namespace_id, sticky.task_queue.clone(), WorkerIdentity("worker".into())).await;
            for copy in 0..copies {
                prop_assert_eq!(node.receive(&fallback, home(&normal), OfferSource::Pushed).await.unwrap(), if copy == 0 { WorkflowOfferOutcome::Admitted } else { WorkflowOfferOutcome::Known });
            }
            prop_assert_eq!(node.broker.backlog_stats(&sticky).await.count, 0);
            prop_assert_eq!(&fallback.hint.sticky_origin, &offer.hint.sticky_origin);
            if !start_before_deadline { tokio::time::advance(Duration::from_secs(30)).await; }
            let claimed = node.broker.try_claim_workflow_task(&normal, offer.hint.task.run_key).await;
            prop_assert_eq!(claimed.is_some(), start_before_deadline);
            if let Some((task, _)) = claimed { prop_assert_eq!(task.sticky_deadline, offer.hint.task.sticky_deadline); }
            tokio::time::advance(Duration::from_secs(30)).await;
            prop_assert_eq!(node.receive(&fallback, home(&normal), OfferSource::Pushed).await.unwrap(), WorkflowOfferOutcome::Expired);
            let LoadedRun::Existing(state) = repo.load_run(offer.hint.task.run_key).await.unwrap() else { panic!("stored run"); };
            prop_assert!(state.sticky.is_some()); prop_assert!(state.pending_workflow_task.unwrap().started_event_id.is_none());
            Ok(())
        })?;
    }

    // Feature: queue-home-delivery, Property 7: push/discovery receipts cannot wait for a publishing lane's start.
    #[test]
    fn property_7_receipts_and_interleaved_discovery(index in 0u32..100, actions in prop::collection::vec(0u8..6, 1..30)) {
        runtime().block_on(async {
            let repo = Arc::new(InMemoryStore::with_shard_count(8)); let clock = clock(); let queue = queue(index);
            let offer = stored(&repo, &queue, None, index, clock.as_ref()).await;
            let node = node(repo.clone(), &[home(&queue)], clock);
            let before = serde_json::to_vec(&repo.load_run(offer.hint.task.run_key).await.unwrap()).unwrap();
            let (ack, mut received) = mpsc::channel(1);
            let (resume, paused_lane) = tokio::sync::oneshot::channel::<()>();
            let publication = offer.clone(); let target = node.clone();
            let lane = tokio::spawn(async move {
                let result = target.receive(&publication, home(&publication.hint.task.queue), OfferSource::Pushed).await.unwrap();
                ack.send(result).await.unwrap();
                paused_lane.await.unwrap();
            });
            prop_assert_eq!(received.recv().await.unwrap(), WorkflowOfferOutcome::Admitted);
            for action in actions {
                match action {
                    0 | 1 => { prop_assert_eq!(node.receive(&offer, home(&queue), OfferSource::Pushed).await.unwrap(), WorkflowOfferOutcome::Known); }
                    2 => { let home = node.homes.local_home(&queue).unwrap(); let mut slots = node.broker.reserve_offers(&home, 1).await; prop_assert_eq!(node.broker.publish_discovered(offer.hint.task.clone(), offer.hint.scheduled_at, &mut slots).await, OfferAdmission::Known); }
                    3 => { prop_assert_eq!(node.receive(&offer, ShardId((home(&queue).0 + 1) % 8), OfferSource::Pushed).await.unwrap(), WorkflowOfferOutcome::NotHome); }
                    4 => { node.homes.owner.write().unwrap().remove(home(&queue)); prop_assert_eq!(node.receive(&offer, home(&queue), OfferSource::Pushed).await.unwrap(), WorkflowOfferOutcome::NotHome); { let mut owner = node.homes.owner.write().unwrap(); owner.record_acquired(home(&queue), ShardEpoch::ZERO); owner.mark_active(home(&queue)); } prop_assert_eq!(node.receive(&offer, home(&queue), OfferSource::Pushed).await.unwrap(), WorkflowOfferOutcome::Admitted); }
                    _ => { let _lost_reply = node.receive(&offer, home(&queue), OfferSource::Pushed).await.unwrap(); }
                }
            }
            resume.send(()).unwrap(); lane.await.unwrap();
            prop_assert_eq!(serde_json::to_vec(&repo.load_run(offer.hint.task.run_key).await.unwrap()).unwrap(), before);
            Ok(())
        })?;
    }
}

#[derive(Debug)]
struct HomeSink {
    nodes: Vec<Arc<WorkflowDelivery<InMemoryStore>>>,
    replies: mpsc::UnboundedSender<(ShardId, WorkflowOfferOutcome)>,
    lose_first: Mutex<bool>,
}
#[async_trait]
impl RemoteWorkflowOfferSink for HomeSink {
    async fn offer(
        &self,
        offer: &WorkflowOffer,
        destination: ShardId,
        _: bool,
    ) -> Result<WorkflowOfferOutcome, WorkflowOfferError> {
        let node = self
            .nodes
            .iter()
            .find(|node| node.homes.local_home(&offer.hint.task.queue).is_some())
            .unwrap();
        let outcome = node
            .receive(offer, destination, OfferSource::Pushed)
            .await?;
        self.replies.send((destination, outcome)).unwrap();
        if std::mem::take(&mut *self.lose_first.lock().unwrap()) {
            return Err(WorkflowOfferError::Transport);
        }
        Ok(outcome)
    }
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: queue-home-delivery, Property 4: three distinct homes route fallback through the sender and retain original intent.
    #[test]
    fn property_4_distinct_homes_fallback_and_lost_reply(index in 0u32..100, lose in any::<bool>()) {
        runtime().block_on(async {
            let repo = Arc::new(InMemoryStore::with_shard_count(8)); let clock = clock();
            let normal = queue(index); let sticky = (index+1..index+100).map(queue).find(|q| home(q) != home(&normal)).unwrap();
            let mut ordinal = index;
            let offer = loop {
                let offer = stored(&repo, &normal, Some(&sticky), ordinal, clock.as_ref()).await;
                if offer.hint.execution_home != home(&sticky) && offer.hint.execution_home != home(&normal) { break offer; }
                ordinal += 1;
            };
            let execution = node(repo.clone(), &[offer.hint.execution_home], clock.clone());
            let sticky_node = node(repo.clone(), &[home(&sticky)], clock.clone()); let normal_node = node(repo.clone(), &[home(&normal)], clock.clone());
            let (tx, mut rx) = mpsc::unbounded_channel();
            execution.install_sender(Arc::new(HomeSink { nodes: vec![sticky_node, normal_node.clone()], replies: tx, lose_first: Mutex::new(lose) }));
            execution.publish(offer.clone(), None).await;
            let mut destinations = Vec::new();
            loop { let (destination, outcome) = rx.recv().await.unwrap(); destinations.push(destination); if destination == home(&normal) && matches!(outcome, WorkflowOfferOutcome::Admitted | WorkflowOfferOutcome::Known) { break; } }
            prop_assert_eq!(destinations[0], home(&sticky));
            let (task, _) = normal_node.broker.try_claim_workflow_task(&normal, offer.hint.task.run_key).await.unwrap();
            prop_assert_eq!(task.sticky_deadline, offer.hint.task.sticky_deadline);
            prop_assert_eq!(task.logical_seq, offer.hint.task.logical_seq);
            let LoadedRun::Existing(state) = repo.load_run(offer.hint.task.run_key).await.unwrap() else { panic!("stored run"); };
            prop_assert!(state.sticky.is_some()); prop_assert!(state.pending_workflow_task.unwrap().started_event_id.is_none());
            execution.shutdown.begin_shutdown();
            Ok(())
        })?;
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: queue-home-delivery, Property 4: ramping input and workflow identity select the receiver's current target; a nil target is not unavailability.
    #[test]
    fn property_4_receiver_ramping_and_recent_window(index in 0u32..100, force_ramping in any::<bool>(), ramp_percent in prop_oneof![Just(0f32), Just(50f32), Just(100f32)], nil_target in any::<bool>()) {
        runtime().block_on(async {
            let repo = Arc::new(InMemoryStore::with_shard_count(8)); let clock = clock(); let normal = queue(index); let sticky = queue(index + 100);
            let mut offer = stored(&repo, &normal, Some(&sticky), index, clock.as_ref()).await;
            offer.directive.deployment = Some(WorkerDeploymentVersionRef { deployment_name: "deploy".into(), build_id: "v1".into() });
            offer.directive.behavior = VersioningBehavior::AutoUpgrade; offer.directive.use_ramping_version = force_ramping;
            let node = node(repo.clone(), &[home(&sticky)], clock);
            registry(&node, &normal, false).await;
            let key = DeploymentKey { namespace_id: normal.namespace_id, deployment_name: DeploymentName("deploy".into()) };
            let mut record = repo.load_deployment(&key).await.unwrap().unwrap(); let token = record.conflict_token;
            record.routing_config.ramping_version = (!nil_target).then(|| tokeira_storage::WorkerDeploymentVersionKey { deployment_name: DeploymentName("deploy".into()), build_id: tokeira_storage::BuildId("v2".into()) });
            record.routing_config.ramping_version_percentage = ramp_percent;
            if nil_target { record.routing_config.current_version = None; }
            let expected_target = if force_ramping && !nil_target { Some("v2") }
                else if nil_target { None }
                else if ramp_percent == 100.0 || (ramp_percent == 50.0 && crate::runtime::workflow_task::routing_config_target(&record.routing_config, &offer.directive.workflow_id).is_some_and(|v| v.build_id == "v2")) { Some("v2") } else { Some("v1") };
            repo.put_deployment(record, Some(token)).await.unwrap();
            recent(&node, &offer).await;
            prop_assert_eq!(node.receive(&offer, home(&sticky), OfferSource::Pushed).await.unwrap(), if expected_target == Some("v2") { WorkflowOfferOutcome::StickyUnavailable(StickyUnavailableReason::DeploymentChanged) } else { WorkflowOfferOutcome::Admitted });
            offer.directive.behavior = VersioningBehavior::Pinned;
            tokio::time::advance(Duration::from_secs(10)).await;
            prop_assert!(matches!(node.receive(&offer, home(&sticky), OfferSource::Pushed).await.unwrap(), WorkflowOfferOutcome::Admitted | WorkflowOfferOutcome::Known));
            tokio::time::advance(Duration::from_nanos(1)).await;
            prop_assert_eq!(node.receive(&offer, home(&sticky), OfferSource::Pushed).await.unwrap(), WorkflowOfferOutcome::StickyUnavailable(StickyUnavailableReason::NoRecentPoller));
            Ok(())
        })?;
    }

    // Feature: queue-home-delivery, Property 7: a full sticky broker remains capacity refusal, never semantic fallback.
    #[test]
    fn property_7_full_sticky_home_never_selects_fallback(index in 0u32..100) {
        runtime().block_on(async {
            let repo = Arc::new(InMemoryStore::with_shard_count(8)); let clock = clock(); let normal = queue(index); let sticky = queue(index + 100);
            let offer = stored(&repo, &normal, Some(&sticky), index, clock.as_ref()).await;
            let node = node(repo.clone(), &[home(&sticky)], clock);
            recent(&node, &offer).await;
            let mut filler = offer.hint.task.clone(); filler.sticky_preferred = None; filler.normal_queue = None;
            for id in 1..=256 {
                filler.run_key = tokeira_types::RunKey(uuid::Uuid::from_u128(id));
                prop_assert_eq!(node.broker.admit_workflow_offer(filler.clone(), OfferSource::Discovered, Some(offer.hint.scheduled_at), None, false).await, OfferAdmission::Admitted);
            }
            let before = repo.snapshot().await.unwrap();
            prop_assert_eq!(node.receive(&offer, home(&sticky), OfferSource::Pushed).await.unwrap(), WorkflowOfferOutcome::Full);
            prop_assert_eq!(repo.snapshot().await.unwrap(), before);
            Ok(())
        })?;
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: queue-home-delivery, Property 4: an absent poller is unavailable before any failing policy read.
    #[test]
    fn property_4_no_poller_precedes_routing_failure(index in 0u32..100, changed in any::<bool>()) {
        runtime().block_on(async {
            let repo = Arc::new(InMemoryStore::with_shard_count(8)); let clock = clock(); let normal = queue(index); let sticky = queue(index + 100);
            let offer = stored(&repo, &normal, Some(&sticky), index, clock.as_ref()).await;
            let node = node(repo, &[home(&sticky)], clock);
            let store = registry(&node, &normal, changed).await;
            store.fail.store(true, Ordering::SeqCst);
            prop_assert_eq!(node.receive(&offer, home(&sticky), OfferSource::Pushed).await, Ok(WorkflowOfferOutcome::StickyUnavailable(StickyUnavailableReason::NoRecentPoller)));
            recent(&node, &offer).await;
            prop_assert_eq!(node.receive(&offer, home(&sticky), OfferSource::Pushed).await, Err(WorkflowOfferError::Routing));
            Ok(())
        })?;
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: queue-home-delivery, Property 4: immediate local admission reuses the publisher's deployment decision, while later attempts recheck policy.
    #[test]
    fn property_4_local_publication_reuses_routing(index in 0u32..100) {
        runtime().block_on(async {
            let repo = Arc::new(InMemoryStore::with_shard_count(8)); let clock = clock(); let normal = queue(index); let sticky = queue(index + 100);
            let offer = stored(&repo, &normal, Some(&sticky), index, clock.as_ref()).await;
            let node = node(repo, &(0..8).map(ShardId).collect::<Vec<_>>(), clock);
            let store = registry(&node, &normal, false).await;
            recent(&node, &offer).await;
            store.fail.store(true, Ordering::SeqCst);
            node.publish(offer.clone(), Some(normal)).await;
            prop_assert!(node.broker.has_runnable_backlog(&sticky).await);
            prop_assert_eq!(node.receive(&offer, home(&sticky), OfferSource::Pushed).await, Err(WorkflowOfferError::Routing));
            prop_assert_eq!(node.receive(&offer, home(&sticky), OfferSource::Local).await, Err(WorkflowOfferError::Routing));
            Ok(())
        })?;
    }
}
