//! Generated recovery schedules over real memory pages, receivers and sender.

use super::*;
use crate::{
    activity_discovery::{ActivityDiscovery, ActivityDiscoveryRegistry, ActivitySource},
    broker::{InMemoryActivityBroker, WorkflowPollResult},
    discovery::{DiscoveryRegistry, WorkflowSource, run_discovery},
    runtime::repair_tests::{Point, Repo, StickyPageGate},
    workflow_delivery::tests::{clock, home, node, queue, recent, runtime, stored},
};
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use proptest::prelude::*;
use std::{
    collections::{BTreeSet, VecDeque},
    sync::atomic::{AtomicUsize, Ordering},
};
use tokeira_kernel::{
    BasicKernel, Command, Kernel, ResetStickyRequest, WorkflowTaskTimedOutRequest,
    WorkflowTaskTimeoutType,
};
use tokeira_storage::{InMemoryStore, WriteFence};
use tokeira_types::{ShardEpoch, StickyAffinity, WorkerIdentity};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

fn counter(recorder: &DebuggingRecorder, name: &str, reason: Option<&str>) -> u64 {
    recorder
        .snapshotter()
        .snapshot()
        .into_vec()
        .into_iter()
        .filter_map(|(key, _, _, value)| {
            if key.key().name() != name
                || reason.is_some_and(|reason| {
                    !key.key()
                        .labels()
                        .any(|label| label.key() == "reason" && label.value() == reason)
                })
            {
                return None;
            }
            if let DebugValue::Counter(count) = value {
                Some(count)
            } else {
                None
            }
        })
        .sum()
}

async fn stop<R: RunRepository + 'static>(node: &WorkflowDelivery<R>) {
    node.shutdown.begin_shutdown();
    node.shutdown
        .wait(std::time::Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
}

async fn seed(repo: &InMemoryStore, key: u128, clock: &dyn Clock, age: Duration) -> WorkflowOffer {
    let normal = queue(0);
    let mut item = crate::discovery::tests::transition(&normal, key as usize);
    let state = &mut item.next_state;
    state.run_key = RunKey(uuid::Uuid::from_u128(key));
    state.sticky = Some(StickyAffinity {
        sticky_queue: queue(100).task_queue,
        worker_identity: WorkerIdentity("worker".into()),
        schedule_to_start_timeout: time::Duration::seconds(30),
    });
    let pending = state.pending_workflow_task.as_mut().unwrap();
    pending.scheduled_at = clock.now() - time::Duration::try_from(age).unwrap();
    pending.schedule_to_start_deadline = Some(pending.scheduled_at + time::Duration::seconds(30));
    let offer = WorkflowOffer::from_state(state, ShardId(0))
        .unwrap()
        .unwrap();
    repo.commit_transition(state.run_key, item, WriteFence::Unfenced)
        .await
        .unwrap();
    offer
}

async fn read_page<R: RunRepository + 'static>(
    delivery: &WorkflowDelivery<R>,
    acquisition: &ShardAcquisition,
    mut cursor: Option<RunKey>,
) -> Result<Option<RunKey>> {
    page(delivery, acquisition, &mut cursor).await?;
    Ok(cursor)
}

#[derive(Debug)]
struct FaultSink {
    nodes: Vec<Arc<WorkflowDelivery<InMemoryStore>>>,
    faults: Mutex<VecDeque<u8>>,
    selected: ShardId,
    replies: mpsc::UnboundedSender<(
        WorkflowOffer,
        Result<WorkflowOfferOutcome, WorkflowOfferError>,
    )>,
}
#[async_trait]
impl RemoteWorkflowOfferSink for FaultSink {
    async fn offer(
        &self,
        offer: &WorkflowOffer,
        destination: ShardId,
        _: bool,
    ) -> Result<WorkflowOfferOutcome, WorkflowOfferError> {
        let fault = if destination == self.selected {
            self.faults.lock().unwrap().pop_front()
        } else {
            None
        };
        let result = match fault {
            Some(0) => Ok(WorkflowOfferOutcome::Full),
            Some(1) => Err(WorkflowOfferError::Transport),
            Some(2) => Ok(WorkflowOfferOutcome::NotHome),
            Some(_) => Err(WorkflowOfferError::Authentication),
            None => {
                let target = self
                    .nodes
                    .iter()
                    .find(|node| node.homes.local_home(&offer.hint.task.queue).is_some())
                    .unwrap();
                target
                    .receive(offer, destination, OfferSource::Pushed)
                    .await
            }
        };
        self.replies.send((offer.clone(), result)).unwrap();
        result
    }
}

async fn lost_push(index: u32, fallback: bool, faults: Vec<u8>, move_home: bool) {
    let repo = Arc::new(InMemoryStore::with_shard_count(8));
    let clock = clock();
    let normal = queue(index);
    let sticky = (index + 1..index + 200)
        .map(queue)
        .find(|q| home(q) != home(&normal))
        .unwrap();
    let offer = (index..index + 200)
        .map(|i| {
            (
                i,
                tokeira_types::execution_home_bundle(
                    normal.namespace_id.0.as_bytes(),
                    format!("discovery-{i}").as_bytes(),
                    8,
                ),
            )
        })
        .find(|(_, h)| *h != home(&normal) && *h != home(&sticky))
        .unwrap();
    let offer = stored(&repo, &normal, Some(&sticky), offer.0, clock.as_ref()).await;
    let sticky_node = node(repo.clone(), &[home(&sticky)], clock.clone());
    let normal_node = node(repo.clone(), &[home(&normal)], clock.clone());
    if !fallback {
        recent(&sticky_node, &offer).await;
    }
    let execution = node(repo.clone(), &[offer.hint.execution_home], clock.clone());
    let selected = if fallback {
        home(&normal)
    } else {
        home(&sticky)
    };
    let (replies, mut outcomes) = mpsc::unbounded_channel();
    let sink = Arc::new(FaultSink {
        nodes: vec![sticky_node.clone(), normal_node.clone()],
        faults: Mutex::new(VecDeque::from([1])),
        selected,
        replies,
    });
    execution.install_sender(sink);
    execution.publish(offer.clone(), None).await;
    loop {
        let (_, result) = outcomes.recv().await.unwrap();
        if result == Err(WorkflowOfferError::Transport) {
            break;
        }
    }
    assert!(!sticky_node.broker.has_runnable_backlog(&sticky).await);
    assert!(!normal_node.broker.has_runnable_backlog(&normal).await);
    stop(&execution).await;
    let before = repo.snapshot().await.unwrap();
    let restored = Arc::new(InMemoryStore::from_snapshot(&before).unwrap());
    let replacement = node(restored, &[offer.hint.execution_home], clock.clone());
    let target = if fallback { &normal_node } else { &sticky_node };
    let moved_homes = if move_home { vec![selected] } else { vec![] };
    let moved = node(repo.clone(), &moved_homes, clock.clone());
    if move_home {
        target.homes.owner.write().unwrap().remove(selected);
    }
    let mut nodes = vec![sticky_node.clone(), normal_node.clone()];
    nodes.push(moved.clone());
    if !fallback && move_home {
        recent(&moved, &offer).await;
    }
    let (replies, mut outcomes) = mpsc::unbounded_channel();
    replacement.install_sender(Arc::new(FaultSink {
        nodes,
        faults: Mutex::new(faults.into()),
        selected,
        replies,
    }));
    replacement.start_sticky_sweep(replacement.acquisition(offer.hint.execution_home).unwrap());
    let delivered = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let (attempt, result) = outcomes.recv().await.unwrap();
            if result == Ok(WorkflowOfferOutcome::Admitted) {
                break attempt;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(
        delivered.hint.task.sticky_deadline,
        offer.hint.task.sticky_deadline
    );
    assert_eq!(delivered.hint.sticky_origin, offer.hint.sticky_origin);
    assert_eq!(
        delivered.hint.task.queue,
        if fallback {
            normal.clone()
        } else {
            sticky.clone()
        }
    );
    let receiver = if move_home { &moved } else { target };
    let task = receiver
        .broker
        .try_claim_workflow_task_for_worker(
            &delivered.hint.task.queue,
            offer.hint.task.run_key,
            Some(&WorkerIdentity("worker".into())),
        )
        .await
        .unwrap()
        .0;
    assert_eq!(task.sticky_deadline, offer.hint.task.sticky_deadline);
    assert_eq!(replacement.repo.snapshot().await.unwrap(), before);
    stop(&replacement).await;
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: queue-home-delivery, Property 4: a lost sticky push survives process and queue-owner replacement, then fair delivery.
    #[test]
    fn property_4_recovery_lost_sticky_push(index in 0u32..100, faults in prop::collection::vec(0u8..4, 0..5), movement in any::<bool>()) {
        runtime().block_on(lost_push(index, false, faults, movement));
    }
    // Feature: queue-home-delivery, Property 4: losing normal fallback leaves sticky intent, which is re-decided without sticky demand.
    #[test]
    fn property_4_recovery_lost_normal_fallback_push(index in 0u32..100, faults in prop::collection::vec(0u8..4, 0..5), movement in any::<bool>()) {
        runtime().block_on(lost_push(index, true, faults, movement));
    }

    // Feature: queue-home-delivery, Property 8: refusal advances every page; wrapping and reacquisition discover insertions behind the cursor.
    #[test]
    fn property_8_recovery_cursor_wrap_and_saturation(count in 65u128..135, behind in 1u128..50) {
        runtime().block_on(async {
            let repo = Arc::new(InMemoryStore::default()); let clock = clock();
            let delivery = node(repo.clone(), &(0..8).map(ShardId).collect::<Vec<_>>(), clock.clone());
            let acquisition = delivery.acquisition(ShardId(0)).unwrap();
            let mut offers = Vec::new();
            for key in 100..100+count { offers.push(seed(&repo, key, clock.as_ref(), sender::TOTAL).await); }
            let sender = sender::WorkflowOfferSender::default();
            let mut filler = offers[0].clone();
            for key in 1000..2024 { filler.hint.task.run_key = RunKey(uuid::Uuid::from_u128(key));
                prop_assert_eq!(sender.try_enqueue(filler.clone(), acquisition.clone(), PublicationSource::Publication, ShardId(7), Instant::now(), false, clock.now()), sender::EnqueueOutcome::Queued); }
            *delivery.sender.write().unwrap() = Some(sender);
            let mut cursor = read_page(&delivery, &acquisition, None).await.unwrap();
            prop_assert_eq!(cursor, Some(RunKey(uuid::Uuid::from_u128(163))));
            let inserted = seed(&repo, behind, clock.as_ref(), sender::TOTAL).await;
            while cursor.is_some() { cursor = read_page(&delivery, &acquisition, cursor).await.unwrap(); }
            prop_assert!(!delivery.broker.has_runnable_backlog(&queue(0)).await);
            *delivery.sender.write().unwrap() = None;
            let before = repo.snapshot().await.unwrap();
            loop { cursor = read_page(&delivery, &acquisition, cursor).await.unwrap(); if cursor.is_none() { break; } }
            offers.push(inserted);
            for offer in &offers { prop_assert!(delivery.broker.try_claim_workflow_task(&queue(0), offer.hint.task.run_key).await.is_some()); }
            prop_assert_eq!(repo.snapshot().await.unwrap(), before);
            Ok(())
        })?;
    }
}

fn gated_repo() -> (
    Arc<Repo>,
    Arc<StickyPageGate>,
    mpsc::UnboundedReceiver<(
        ShardId,
        Option<RunKey>,
        u32,
        Instant,
        tokio::sync::oneshot::Sender<()>,
    )>,
) {
    let (entered, arrivals) = mpsc::unbounded_channel();
    let gate = Arc::new(StickyPageGate {
        entered,
        active: AtomicUsize::new(0),
    });
    let repo = Arc::new(Repo::with_inner(InMemoryStore::default()));
    *repo.sticky_gate.lock().unwrap() = Some(gate.clone());
    (repo, gate, arrivals)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: queue-home-delivery, Property 8: FIFO page permits bound concurrent reads and give every home a turn before repeat pages.
    #[test]
    fn property_8_recovery_fair_bounded_scheduler(homes in 9u32..25, reverse in any::<bool>()) {
        runtime().block_on(async {
            let (repo, gate, mut arrivals) = gated_repo();
            let delivery = node(repo, &(0..homes).map(ShardId).collect::<Vec<_>>(), clock());
            let mut order: Vec<_> = (0..homes).collect(); if reverse { order.reverse(); }
            for id in order { let a = delivery.acquisition(ShardId(id)).unwrap(); delivery.start_sticky_sweep(a.clone()); delivery.start_sticky_sweep(a); }
            let mut seen = BTreeSet::new(); let mut held = VecDeque::new(); let started = Instant::now();
            for _ in 0..8 { let (home, cursor, limit, _, resume) = arrivals.recv().await.unwrap();
                prop_assert!(seen.insert(home)); prop_assert_eq!(cursor, None); prop_assert_eq!(limit, 64); held.push_back(resume); }
            prop_assert_eq!(gate.active.load(Ordering::SeqCst), 8);
            prop_assert!(arrivals.try_recv().is_err());
            while seen.len() < homes as usize {
                held.pop_front().unwrap().send(()).unwrap();
                let (home, _, _, _, resume) = arrivals.recv().await.unwrap();
                prop_assert!(seen.insert(home)); held.push_back(resume);
                prop_assert!(gate.active.load(Ordering::SeqCst) <= 8);
            }
            for resume in held { resume.send(()).unwrap(); }
            let (_, _, _, at, _) = arrivals.recv().await.unwrap();
            prop_assert!(at >= started + PERIOD);
            stop(&delivery).await;
            prop_assert_eq!(gate.active.load(Ordering::SeqCst), 0);
            while arrivals.try_recv().is_ok() {}
            tokio::time::advance(PERIOD).await;
            prop_assert!(arrivals.try_recv().is_err());
            Ok(())
        })?;
    }

    // Feature: queue-home-delivery, Property 8: a middle-row hydration failure retries that row after backoff without rereading or reoffering the completed prefix.
    #[test]
    fn property_8_recovery_incomplete_read_retries(hydration in any::<bool>(), faults in 1usize..4, middle in 165u128..180) {
        let recorder = DebuggingRecorder::new();
        metrics::with_local_recorder(&recorder, || runtime().block_on(async {
            let (repo, _, mut arrivals) = gated_repo(); let clock = clock();
            for key in 100..181 { seed(&repo.inner, key, clock.as_ref(), sender::TOTAL).await; }
            *repo.observed_loads.lock().unwrap() = Some(Vec::new());
            let delivery = node(repo.clone(), &(0..8).map(ShardId).collect::<Vec<_>>(), clock);
            delivery.start_sticky_sweep(delivery.acquisition(ShardId(0)).unwrap());
            let (_, cursor, _, _, resume) = arrivals.recv().await.unwrap(); prop_assert_eq!(cursor, None); resume.send(()).unwrap();
            let mut last = Instant::now();
            for failure in 0..faults {
                let (_, cursor, _, at, resume) = arrivals.recv().await.unwrap();
                let after = if hydration && failure > 0 { middle - 1 } else { 163 };
                prop_assert_eq!(cursor, Some(RunKey(uuid::Uuid::from_u128(after))));
                prop_assert!(at - last >= PERIOD * (1 << failure)); last = at;
                if hydration { *repo.fail_run_load.lock().unwrap() = Some(RunKey(uuid::Uuid::from_u128(middle))); }
                else { *repo.fail.lock().unwrap() = Some(Point::StickyPage); }
                resume.send(()).unwrap();
            }
            let (_, cursor, _, at, resume) = arrivals.recv().await.unwrap();
            prop_assert_eq!(cursor, Some(RunKey(uuid::Uuid::from_u128(if hydration { middle - 1 } else { 163 }))));
            prop_assert!(at - last >= PERIOD * (1 << faults));
            *repo.fail_run_load.lock().unwrap() = None;
            resume.send(()).unwrap();
            let (_, cursor, _, _, _) = arrivals.recv().await.unwrap(); prop_assert_eq!(cursor, None);
            let reads = repo.observed_loads.lock().unwrap().clone().unwrap();
            for key in 100..181 {
                let loads = reads.iter().filter(|run| run.0.as_u128() == key).count();
                prop_assert_eq!(loads, if hydration && key == middle { faults + 1 } else { 1 }, "run {}", key);
            }
            prop_assert!(delivery.broker.try_claim_workflow_task(&queue(0), RunKey(uuid::Uuid::from_u128(180))).await.is_some());
            prop_assert_eq!(counter(&recorder, crate::metrics::WORKFLOW_OFFER_SWEEP_ENQUEUED_TOTAL, None), 81);
            stop(&delivery).await;
            Ok(())
        }))?;
    }

    // Feature: queue-home-delivery, Property 8: a cancelled read cannot install an offer in its replacement, which starts at the beginning.
    #[test]
    fn property_8_recovery_cancelled_acquisition(by_deadline in any::<bool>(), after_load in any::<bool>()) {
        runtime().block_on(async {
            let repo = Arc::new(Repo::with_inner(InMemoryStore::default())); let clock = clock();
            let offer = seed(&repo.inner, 100, clock.as_ref(), sender::TOTAL).await;
            let delivery = node(repo.clone(), &(0..8).map(ShardId).collect::<Vec<_>>(), clock);
            let old = delivery.acquisition(ShardId(0)).unwrap();
            *old.deadline.lock().unwrap() = Some(Instant::now() + Duration::from_millis(100));
            let (entered, resume) = repo.pause(if after_load { Point::Loaded } else { Point::StickyRead });
            let old_task = delivery.shutdown.spawn(run(delivery.clone(), old.clone())); entered.await.unwrap();
            if by_deadline { tokio::time::advance(Duration::from_millis(100)).await; } else { old.cancel.cancel(); }
            let replacement = { let mut owner = delivery.homes.owner.write().unwrap(); owner.record_acquired(ShardId(0), ShardEpoch::ZERO); owner.mark_active(ShardId(0)); owner.acquisition(ShardId(0)).unwrap() };
            delivery.start_sticky_sweep(old);
            let _ = resume.send(());
            old_task.await.unwrap();
            prop_assert!(!delivery.broker.has_runnable_backlog(&queue(0)).await);
            prop_assert!(!replacement.cancel.is_cancelled());
            delivery.start_sticky_sweep(replacement);
            let found = delivery.broker.poll_workflow_task(&queue(0), &WorkerIdentity("normal".into()), Duration::from_secs(2)).await.unwrap();
            prop_assert!(matches!(found, Some(WorkflowPollResult::Queued(task, _)) if task.run_key == offer.hint.task.run_key));
            stop(&delivery).await;
            Ok(())
        })?;
    }
}

async fn normal_discovery<R: RunRepository + 'static>(
    delivery: Arc<WorkflowDelivery<R>>,
    queue: QueueKey,
) -> WorkflowPollResult {
    let registry = DiscoveryRegistry::default();
    let _demand = registry.register_poll(
        queue.clone(),
        delivery.homes.local_home(&queue).unwrap(),
        Instant::now(),
    );
    let cancel = CancellationToken::new();
    let task = tokio::spawn(run_discovery(
        WorkflowSource {
            repo: delivery.repo.clone(),
            registry: delivery.registry.clone(),
        },
        registry,
        delivery.broker.clone(),
        ActivityDiscovery {
            source: Arc::new(ActivitySource {
                repo: delivery.repo.clone(),
                registry: delivery.registry.clone(),
                owner: delivery.homes.owner.clone(),
            }),
            registry: ActivityDiscoveryRegistry::default(),
            broker: InMemoryActivityBroker::with_discovery(delivery.homes.clone()),
            clock: delivery.clock.clone(),
        },
        cancel.clone(),
    ));
    let found = delivery
        .broker
        .poll_workflow_task(
            &queue,
            &WorkerIdentity("normal".into()),
            Duration::from_secs(3),
        )
        .await
        .unwrap()
        .unwrap();
    cancel.cancel();
    task.await.unwrap();
    found
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: queue-home-delivery, Property 4: expiry writes nothing; cleared affinity and timeout successors are recovered only by normal discovery.
    #[test]
    fn property_4_recovery_expiry_and_cleared_affinity(timeout in any::<bool>(), key in 1u128..100) {
        runtime().block_on(async {
            let repo = Arc::new(InMemoryStore::default()); let clock = clock(); let offer = seed(&repo, key, clock.as_ref(), sender::TOTAL).await;
            let delivery = node(repo.clone(), &(0..8).map(ShardId).collect::<Vec<_>>(), clock.clone());
            let acquisition = delivery.acquisition(ShardId(0)).unwrap();
            if timeout { tokio::time::advance(Duration::from_secs(30)).await;
                let before = repo.snapshot().await.unwrap(); read_page(&delivery, &acquisition, None).await.unwrap(); prop_assert_eq!(repo.snapshot().await.unwrap(), before);
                prop_assert!(!delivery.broker.has_runnable_backlog(&queue(0)).await);
            }
            let loaded = repo.load_run(offer.hint.task.run_key).await.unwrap();
            let command = if timeout { Command::WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest { logical_seq: offer.hint.task.logical_seq, started_event_id: 0, timeout_type: WorkflowTaskTimeoutType::ScheduleToStart, now: clock.now() }) }
                else { Command::ResetSticky(ResetStickyRequest { now: clock.now() }) };
            let transition = BasicKernel.apply(loaded, command).unwrap();
            repo.commit_transition(offer.hint.task.run_key, transition, WriteFence::Unfenced).await.unwrap();
            let before = repo.snapshot().await.unwrap(); read_page(&delivery, &acquisition, None).await.unwrap();
            prop_assert!(!delivery.broker.has_runnable_backlog(&queue(0)).await);
            let found = normal_discovery(delivery, queue(0)).await;
            prop_assert!(matches!(found, WorkflowPollResult::Queued(task, _) if task.run_key == offer.hint.task.run_key));
            prop_assert_eq!(repo.snapshot().await.unwrap(), before);
            Ok(())
        })?;
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: queue-home-delivery, Property 1: after finite read faults and saturation, the sticky reader alone offers every pending task.
    #[test]
    fn property_1_recovery_finite_fault_prefix_fair_suffix(count in 1u128..17, faults in prop::collection::vec(any::<bool>(), 1..6)) {
        runtime().block_on(async {
            let repo = Arc::new(Repo::with_inner(InMemoryStore::default())); let clock = clock();
            let mut offers = Vec::new();
            for key in 1..=count { offers.push(seed(&repo.inner, key, clock.as_ref(), sender::TOTAL).await); }
            let delivery = node(repo.clone(), &(0..8).map(ShardId).collect::<Vec<_>>(), clock);
            let acquisition = delivery.acquisition(ShardId(0)).unwrap();
            let before = repo.inner.snapshot().await.unwrap();
            for hydration in faults { *repo.fail.lock().unwrap() = Some(if hydration { Point::Load } else { Point::StickyPage });
                prop_assert!(read_page(&delivery, &acquisition, None).await.is_err()); }
            let mut filler = offers[0].hint.task.clone(); filler.queue = queue(0); filler.sticky_preferred = None; filler.normal_queue = None;
            for key in 1000..1256 { filler.run_key = RunKey(uuid::Uuid::from_u128(key));
                prop_assert_eq!(delivery.broker.admit_workflow_offer(filler.clone(), OfferSource::Local, None, None, false).await, OfferAdmission::Admitted); }
            prop_assert_eq!(read_page(&delivery, &acquisition, None).await.unwrap(), None);
            for offer in &offers { prop_assert!(delivery.broker.try_claim_workflow_task(&queue(0), offer.hint.task.run_key).await.is_none()); }
            tokio::time::advance(Duration::from_secs(5)).await;
            delivery.broker.expire_offers(Instant::now()).await;
            prop_assert_eq!(read_page(&delivery, &acquisition, None).await.unwrap(), None);
            for offer in offers { prop_assert!(delivery.broker.try_claim_workflow_task(&queue(0), offer.hint.task.run_key).await.is_some()); }
            prop_assert_eq!(repo.inner.snapshot().await.unwrap(), before);
            Ok(())
        })?;
    }

    // Feature: queue-home-delivery, Property 8: an incarnation or intent changed after the page read is skipped without stopping its cursor.
    #[test]
    fn property_8_recovery_stale_candidate(incarnation in any::<bool>(), changed in 100u128..164) {
        runtime().block_on(async {
            let repo = Arc::new(Repo::with_inner(InMemoryStore::default())); let clock = clock();
            let mut offers = Vec::new();
            for key in 100..164 { offers.push(seed(&repo.inner, key, clock.as_ref(), sender::TOTAL).await); }
            let delivery = node(repo.clone(), &(0..8).map(ShardId).collect::<Vec<_>>(), clock.clone());
            let acquisition = delivery.acquisition(ShardId(0)).unwrap();
            let (entered, resume) = repo.pause(Point::StickyRead);
            let scanning = delivery.clone(); let a = acquisition.clone();
            let task = tokio::spawn(async move { read_page(&scanning, &a, None).await });
            entered.await.unwrap();
            let offer = &offers[(changed - 100) as usize];
            let command = if incarnation { Command::WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest { logical_seq: offer.hint.task.logical_seq, started_event_id: 0, timeout_type: WorkflowTaskTimeoutType::ScheduleToStart, now: clock.now() + time::Duration::seconds(30) }) }
                else { Command::ResetSticky(ResetStickyRequest { now: clock.now() }) };
            let transition = BasicKernel.apply(repo.inner.load_run(offer.hint.task.run_key).await.unwrap(), command).unwrap();
            repo.inner.commit_transition(offer.hint.task.run_key, transition, WriteFence::Unfenced).await.unwrap();
            let before = repo.inner.snapshot().await.unwrap();
            resume.send(()).unwrap();
            prop_assert_eq!(task.await.unwrap().unwrap(), Some(RunKey(uuid::Uuid::from_u128(163))));
            for item in &offers { prop_assert_eq!(delivery.broker.try_claim_workflow_task(&queue(0), item.hint.task.run_key).await.is_some(), item.hint.task.run_key != offer.hint.task.run_key); }
            prop_assert_eq!(repo.inner.snapshot().await.unwrap(), before);
            Ok(())
        })?;
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: queue-home-delivery, Property 8: replacing a continued traversal resets its cursor without resetting the home's query rate.
    #[test]
    fn property_8_recovery_reacquisition_restarts_at_head(count in 65u128..90) {
        runtime().block_on(async {
            let (repo, gate, mut arrivals) = gated_repo(); let clock = clock();
            for key in 100..100+count { seed(&repo.inner, key, clock.as_ref(), sender::TOTAL).await; }
            let delivery = node(repo, &(0..8).map(ShardId).collect::<Vec<_>>(), clock);
            delivery.start_sticky_sweep(delivery.acquisition(ShardId(0)).unwrap());
            let (_, cursor, _, first_at, resume) = arrivals.recv().await.unwrap(); prop_assert_eq!(cursor, None); resume.send(()).unwrap();
            let (_, cursor, _, continued_at, old_resume) = arrivals.recv().await.unwrap();
            prop_assert_eq!(cursor, Some(RunKey(uuid::Uuid::from_u128(163))));
            prop_assert!(continued_at >= first_at + PERIOD);
            let replacement = { let mut owner = delivery.homes.owner.write().unwrap(); owner.record_acquired(ShardId(0), ShardEpoch::ZERO); owner.mark_active(ShardId(0)); owner.acquisition(ShardId(0)).unwrap() };
            delivery.start_sticky_sweep(replacement);
            let (_, cursor, _, replaced_at, _) = arrivals.recv().await.unwrap();
            prop_assert_eq!(cursor, None);
            prop_assert!(replaced_at >= continued_at + PERIOD);
            prop_assert!(old_resume.send(()).is_err());
            stop(&delivery).await;
            prop_assert_eq!(gate.active.load(Ordering::SeqCst), 0);
            Ok(())
        })?;
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: queue-home-delivery, Property 8: recent intent incurs no hydration, then wrapping recovers it at the publication budget boundary.
    #[test]
    fn property_8_recovery_recent_rows_skip_hydration(age_ms in 0u64..1000, key in 1u128..100) {
        let recorder = DebuggingRecorder::new();
        metrics::with_local_recorder(&recorder, || runtime().block_on(async {
            let repo = Arc::new(Repo::with_inner(InMemoryStore::default())); let clock = clock();
            let offer = seed(&repo.inner, key, clock.as_ref(), Duration::from_millis(age_ms)).await;
            let delivery = node(repo.clone(), &(0..8).map(ShardId).collect::<Vec<_>>(), clock);
            let acquisition = delivery.acquisition(ShardId(0)).unwrap();
            *repo.observed_loads.lock().unwrap() = Some(Vec::new());
            let before = repo.inner.snapshot().await.unwrap();
            let mut cursor = None;
            page(&delivery, &acquisition, &mut cursor).await.unwrap();
            prop_assert_eq!(cursor, None);
            prop_assert!(repo.observed_loads.lock().unwrap().as_ref().unwrap().is_empty());
            prop_assert_eq!(counter(&recorder, crate::metrics::WORKFLOW_OFFER_SWEEP_SKIPPED_TOTAL, Some("recent")), 1);
            prop_assert!(!delivery.broker.has_runnable_backlog(&queue(0)).await);
            tokio::time::advance(sender::TOTAL - Duration::from_millis(age_ms)).await;
            page(&delivery, &acquisition, &mut cursor).await.unwrap();
            prop_assert_eq!(repo.observed_loads.lock().unwrap().clone().unwrap(), vec![offer.hint.task.run_key]);
            let task = delivery.broker.try_claim_workflow_task(&queue(0), offer.hint.task.run_key).await.unwrap().0;
            prop_assert_eq!(task.sticky_deadline, offer.hint.task.sticky_deadline);
            prop_assert_eq!(repo.inner.snapshot().await.unwrap(), before);
            Ok(())
        }))?;
    }

    // Feature: queue-home-delivery, Property 8: expired intent is skipped before hydration and remains for the authoritative timeout transition.
    #[test]
    fn property_8_recovery_expired_rows_skip_hydration(overdue_ms in 0u64..5000, key in 1u128..100) {
        let recorder = DebuggingRecorder::new();
        metrics::with_local_recorder(&recorder, || runtime().block_on(async {
            let repo = Arc::new(Repo::with_inner(InMemoryStore::default())); let clock = clock();
            seed(&repo.inner, key, clock.as_ref(), Duration::from_millis(30000 + overdue_ms)).await;
            let delivery = node(repo.clone(), &(0..8).map(ShardId).collect::<Vec<_>>(), clock);
            let acquisition = delivery.acquisition(ShardId(0)).unwrap();
            *repo.observed_loads.lock().unwrap() = Some(Vec::new());
            let before = repo.inner.snapshot().await.unwrap();
            let mut cursor = None;
            page(&delivery, &acquisition, &mut cursor).await.unwrap();
            prop_assert_eq!(cursor, None);
            prop_assert!(repo.observed_loads.lock().unwrap().as_ref().unwrap().is_empty());
            prop_assert_eq!(counter(&recorder, crate::metrics::WORKFLOW_OFFER_SWEEP_SKIPPED_TOTAL, Some("expired")), 1);
            prop_assert!(!delivery.broker.has_runnable_backlog(&queue(0)).await);
            prop_assert_eq!(repo.inner.snapshot().await.unwrap(), before);
            Ok(())
        }))?;
    }

    // Feature: queue-home-delivery, Property 4: a no-sender fallback reaches the normal queue with one hydration per run per pass.
    #[test]
    fn property_4_recovery_local_fallback_reuses_hydration(age_ms in 1000u64..20000, key in 1u128..100, passes in 1usize..5) {
        runtime().block_on(async {
            let repo = Arc::new(Repo::with_inner(InMemoryStore::default())); let clock = clock();
            let offer = seed(&repo.inner, key, clock.as_ref(), Duration::from_millis(age_ms)).await;
            let delivery = node(repo.clone(), &(0..8).map(ShardId).collect::<Vec<_>>(), clock);
            let acquisition = delivery.acquisition(ShardId(0)).unwrap();
            *repo.observed_loads.lock().unwrap() = Some(Vec::new());
            let before = repo.inner.snapshot().await.unwrap();
            let mut cursor = None;
            for pass in 1..=passes {
                page(&delivery, &acquisition, &mut cursor).await.unwrap();
                prop_assert_eq!(repo.observed_loads.lock().unwrap().clone().unwrap(), vec![offer.hint.task.run_key; pass]);
                let (task, stamp) = delivery.broker.try_claim_workflow_task(&queue(0), offer.hint.task.run_key).await.unwrap();
                prop_assert_eq!(task.sticky_deadline, offer.hint.task.sticky_deadline);
                delivery.broker.finish_offer((task.run_key, task.logical_seq), stamp).await;
            }
            prop_assert_eq!(repo.inner.snapshot().await.unwrap(), before);
            Ok(())
        })?;
    }
}
