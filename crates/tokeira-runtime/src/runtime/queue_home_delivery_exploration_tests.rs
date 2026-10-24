//! Generated counterexamples for delivery before the push implementation lands.
//!
//! Each property asserts the desired contract. The outer harness accepts only
//! its named invariant failure, retaining a deterministic seed and the shrunk
//! schedule. A panic, rejection, or unexpected pass is a test failure. Remove
//! the expected-failure wrapper as each corresponding fix lands.

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
    (InMemoryBroker::with_discovery(homes.clone()), homes)
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

async fn publication(copies: u32, sticky: bool, colocated: bool) -> bool {
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
            .schedule_to_start_deadline = Some(OffsetDateTime::now_utc() + Duration::seconds(30));
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
    let (destination, _) = broker(home(selected));
    // A completed recent poll is enough; there need not be a parked waiter.
    destination
        .poll_workflow_task(selected, &worker, std::time::Duration::ZERO)
        .await
        .unwrap();
    let source = if colocated {
        destination.clone()
    } else {
        broker(execution).0
    };
    let publisher = publisher(source, repo.clone());
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
    }
    // No discovery scheduler is driven in this fixture. It isolates prompt
    // publication; recovery tests must independently drive the correct reader.
    destination
        .try_claim_workflow_task_for_worker(selected, key, Some(&worker))
        .await
        .is_some()
}

// Feature: queue-home-delivery, Property 1: Prompt push and eventual delivery.
#[test]
fn queue_home_delivery_exploration_normal_split_home() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    assert!(
        rt.block_on(publication(1, false, true)),
        "local positive control"
    );
    expected_failure(
        "normal publication did not reach the queue home",
        |copies| rt.block_on(publication(copies, false, false)),
    );
}

// Feature: queue-home-delivery, Property 4: Sticky routing and deadline preservation.
#[test]
fn queue_home_delivery_exploration_sticky_split_home() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    assert!(
        rt.block_on(publication(1, true, true)),
        "recent-poller positive control"
    );
    expected_failure(
        "sticky publication did not reach the recent poller's home",
        |copies| rt.block_on(publication(copies, true, false)),
    );
}

// Feature: queue-home-delivery, Property 5: Acquisition retirement is immediate and isolated.
#[test]
fn queue_home_delivery_exploration_retirement_without_discovery() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    expected_failure(
        "retired acquisition still exposes a ready offer",
        |copies| {
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
        },
    );
}
