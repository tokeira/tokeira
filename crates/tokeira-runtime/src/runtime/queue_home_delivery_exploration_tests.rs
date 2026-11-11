//! Publication exploration and explicit disconnected-sender negative controls.
//! The same stored intent reaches split homes only when the sender is wired;
//! no discovery loop participates, and receipts synchronize every attempt.

use super::*;
use crate::{
    discovery::{PlacementQueueHomes, QueueHomeProvider, tests::transition},
    lane::DispatchPublisher,
    nexus::{NexusCompletionRuntimeConfig, NoopNexusCompletionClient},
};
use proptest::{
    prelude::*,
    test_runner::{Config, RngAlgorithm, TestError, TestRng, TestRunner},
};
use tokeira_storage::{InMemoryStore, WriteFence};
use tokeira_types::{StickyAffinity, bundle_for_partition, workflow_queue_partition};

#[derive(Debug)]
struct ExplorationClock;
impl Clock for ExplorationClock {
    fn now(&self) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(2_000_000_000).unwrap()
    }
}
fn exploration_clock() -> Arc<dyn Clock> {
    Arc::new(ExplorationClock)
}

const SEED: [u8; 32] = [17; 32];

fn expected_failure(name: &'static str, check: impl Fn(u32) -> bool) {
    let mut runner = TestRunner::new_with_rng(
        Config {
            cases: 100,
            failure_persistence: None,
            ..Config::default()
        },
        TestRng::from_seed(RngAlgorithm::ChaCha, &SEED),
    );
    let result = runner.run(&(1u32..8), |copies| {
        if check(copies) {
            Ok(())
        } else {
            Err(TestCaseError::fail(name))
        }
    });
    match result {
        Err(TestError::Fail(reason, shrunk)) => {
            assert_eq!(reason.to_string(), name);
            assert_eq!(shrunk, 1, "keep the minimal counterexample explicit");
        }
        other => panic!("expected only {name}, seed {SEED:?}; got {other:?}"),
    }
}

fn queue(name: &str) -> QueueKey {
    QueueKey {
        namespace_id: NamespaceId(Uuid::from_u128(17)),
        task_queue: TaskQueueName(name.into()),
        task_kind: TaskKind::Workflow,
        deployment: None,
        build_id: None,
    }
}

fn home(queue: &QueueKey) -> ShardId {
    bundle_for_partition(
        workflow_queue_partition(queue.namespace_id, &queue.task_queue.0, 8),
        8,
    )
}

fn broker(home: ShardId) -> (InMemoryBroker, Arc<PlacementQueueHomes>) {
    let mut owner = ShardOwner::new(8);
    owner.record_acquired(home, ShardEpoch::ZERO);
    owner.mark_active(home);
    let homes = Arc::new(PlacementQueueHomes {
        owner: Arc::new(RwLock::new(owner)),
        counts: RwLock::new((8, 8)),
    });
    (
        InMemoryBroker::with_discovery(homes.clone()).with_clock(exploration_clock()),
        homes,
    )
}

fn publisher(
    broker: InMemoryBroker,
    repo: Arc<InMemoryStore>,
) -> RuntimeDispatchPublisher<InMemoryStore> {
    RuntimeDispatchPublisher::new(
        broker,
        InMemoryActivityBroker::default(),
        repo,
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
}

async fn publication(copies: u32, sticky: bool, colocated: bool, connected: bool) -> bool {
    let repo = Arc::new(InMemoryStore::with_shard_count(8));
    let normal = queue("normal");
    let sticky_queue = (0..100)
        .map(|i| queue(&format!("sticky-{i}")))
        .find(|q| home(q) != home(&normal))
        .unwrap();
    let selected = if sticky { &sticky_queue } else { &normal };
    let mut item = (0..100)
        .map(|i| transition(&normal, i))
        .find(|item| {
            let h = execution_home_bundle(
                normal.namespace_id.0.as_bytes(),
                item.next_state.workflow_id.0.as_bytes(),
                8,
            );
            h != home(&normal) && h != home(&sticky_queue)
        })
        .unwrap();
    let execution = execution_home_bundle(
        normal.namespace_id.0.as_bytes(),
        item.next_state.workflow_id.0.as_bytes(),
        8,
    );
    let key = item.next_state.run_key;
    let worker = WorkerIdentity("recent-sticky-worker".into());
    if sticky {
        item.next_state.sticky = Some(StickyAffinity {
            worker_identity: worker.clone(),
            sticky_queue: sticky_queue.task_queue.clone(),
            schedule_to_start_timeout: Duration::seconds(30),
        });
        item.next_state
            .pending_workflow_task
            .as_mut()
            .unwrap()
            .schedule_to_start_deadline = Some(exploration_clock().now() + Duration::seconds(30));
    }
    let logical_seq = item
        .next_state
        .pending_workflow_task
        .as_ref()
        .unwrap()
        .logical_seq;
    repo.commit_transition(key, item, WriteFence::Unfenced)
        .await
        .unwrap();
    let (destination, destination_homes) = broker(home(selected));
    // A completed recent poll is enough; there need not be a parked waiter.
    destination
        .poll_workflow_task(selected, &worker, std::time::Duration::ZERO)
        .await
        .unwrap();
    let (source, source_homes) = if colocated {
        (destination.clone(), destination_homes.clone())
    } else {
        broker(execution)
    };
    let mut publisher = publisher(source.clone(), repo.clone());
    let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
    let receiver_shutdown = RuntimeShutdownHandle::new();
    let sender_shutdown = RuntimeShutdownHandle::new();
    let receiver = crate::workflow_delivery::WorkflowDelivery::new(
        repo.clone(),
        Arc::new(RwLock::new(None)),
        destination.clone(),
        destination_homes,
        receiver_shutdown.clone(),
        exploration_clock(),
    );
    let delivery = crate::workflow_delivery::WorkflowDelivery::new(
        repo.clone(),
        Arc::new(RwLock::new(None)),
        source,
        source_homes,
        sender_shutdown.clone(),
        exploration_clock(),
    );
    if connected && !colocated {
        delivery.install_sender(Arc::new(DirectSink {
            receiver: receiver.clone(),
            sent,
        }));
        publisher = publisher.with_workflow_delivery(delivery.clone());
    }
    for _ in 0..copies {
        publisher
            .publish(
                key,
                execution,
                &[DispatchOp::EnqueueWorkflowTask {
                    queue: selected.clone(),
                    logical_seq,
                    sticky_preferred: sticky.then(|| worker.clone()),
                    normal_task_queue: sticky.then(|| normal.task_queue.clone()),
                    speculative: false,
                    priority: None,
                }],
            )
            .await
            .unwrap();
        if connected && !colocated {
            assert!(matches!(
                received.recv().await.unwrap(),
                crate::WorkflowOfferOutcome::Admitted | crate::WorkflowOfferOutcome::Known
            ));
        }
    }
    // No discovery scheduler is driven in this fixture. It isolates prompt
    // publication; recovery tests must independently drive the correct reader.
    let delivered = destination
        .try_claim_workflow_task_for_worker(selected, key, Some(&worker))
        .await
        .is_some();
    sender_shutdown.begin_shutdown();
    receiver_shutdown.begin_shutdown();
    sender_shutdown
        .wait(std::time::Instant::now() + std::time::Duration::from_secs(2))
        .await
        .unwrap();
    receiver_shutdown
        .wait(std::time::Instant::now() + std::time::Duration::from_secs(2))
        .await
        .unwrap();
    delivered
}

// Feature: queue-home-delivery, Property 1: Prompt push and eventual delivery.
#[test]
fn queue_home_delivery_negative_control_disconnected_normal() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    assert!(
        rt.block_on(publication(1, false, true, true)),
        "local positive control"
    );
    expected_failure(
        "normal publication did not reach the queue home",
        |copies| rt.block_on(publication(copies, false, false, false)),
    );
}

// Feature: queue-home-delivery, Property 4: Sticky routing and deadline preservation.
#[test]
fn queue_home_delivery_negative_control_disconnected_sticky() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    assert!(
        rt.block_on(publication(1, true, true, true)),
        "recent-poller positive control"
    );
    expected_failure(
        "sticky publication did not reach the recent poller's home",
        |copies| rt.block_on(publication(copies, true, false, false)),
    );
}

// Feature: queue-home-delivery, Property 5: Acquisition retirement is immediate and isolated.
#[test]
fn queue_home_delivery_exploration_retirement_without_discovery() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for copies in 1..=100 {
        assert!({
            rt.block_on(async {
                let repo = Arc::new(InMemoryStore::with_shard_count(8));
                let queue = queue("retirement");
                let item = transition(&queue, 0);
                let key = item.next_state.run_key;
                let seq = item
                    .next_state
                    .pending_workflow_task
                    .as_ref()
                    .unwrap()
                    .logical_seq;
                let task = super::discovery_tests::offer(&item);
                repo.commit_transition(key, item, WriteFence::Unfenced)
                    .await
                    .unwrap();
                let (broker, homes) = broker(home(&queue));
                for _ in 0..copies {
                    broker.publish_workflow_task(task.clone(), None).await;
                }
                assert!(broker.knows_offer((key, seq)).await);
                homes.local_home(&queue).unwrap().cancel.cancel();
                broker.try_claim_workflow_task(&queue, key).await.is_none()
            })
        });
    }
}

#[derive(Debug)]
struct DirectSink {
    receiver: Arc<crate::workflow_delivery::WorkflowDelivery<InMemoryStore>>,
    sent: tokio::sync::mpsc::UnboundedSender<crate::WorkflowOfferOutcome>,
}
#[async_trait::async_trait]
impl crate::RemoteWorkflowOfferSink for DirectSink {
    async fn offer(
        &self,
        offer: &crate::WorkflowOffer,
        home: ShardId,
        _: bool,
    ) -> std::result::Result<crate::WorkflowOfferOutcome, crate::WorkflowOfferError> {
        let outcome = self
            .receiver
            .receive(offer, home, crate::workflow_offers::OfferSource::Pushed)
            .await?;
        self.sent.send(outcome).unwrap();
        Ok(outcome)
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: queue-home-delivery, Property 1: normal publication reaches a split home without discovery.
    #[test]
    fn queue_home_delivery_exploration_normal_split_home(copies in 1u32..8) {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        prop_assert!(rt.block_on(publication(copies, false, false, true)));
    }
    // Feature: queue-home-delivery, Property 4: sticky publication reaches the recent poller's split home without timeout.
    #[test]
    fn queue_home_delivery_exploration_sticky_split_home(copies in 1u32..8) {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        prop_assert!(rt.block_on(publication(copies, true, false, true)));
    }
}
