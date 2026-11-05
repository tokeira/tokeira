//! Actual shared scheduler saturation with storage queries held at their boundary.

use std::collections::{HashSet, VecDeque};

use proptest::prelude::*;
use tokio::sync::mpsc;

use super::*;

type Query = (TaskKind, TaskQueueName, oneshot::Sender<()>);

#[derive(Debug)]
pub(super) struct Gate {
    entered: mpsc::UnboundedSender<Query>,
    active: AtomicUsize,
    maximum: AtomicUsize,
}

#[derive(Debug)]
pub(super) struct Guard(Arc<Gate>);

impl Drop for Guard {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Gate {
    pub(super) async fn enter(self: &Arc<Self>, kind: TaskKind, queue: TaskQueueName) -> Guard {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.maximum.fetch_max(active, Ordering::SeqCst);
        let guard = Guard(self.clone());
        let (resume, waiting) = oneshot::channel();
        self.entered.send((kind, queue, resume)).unwrap();
        let _ = waiting.await;
        guard
    }
}

// Feature: activity-dispatch, Property 4
// Both task kinds progress through one eight-query budget, including cancellation while all slots are held.
proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    #[test]
    fn activity_and_workflow_share_fair_bounded_query_slots(families in 5usize..15, reverse in any::<bool>()) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let (entered, mut arrivals) = mpsc::unbounded_channel();
            let gate = Arc::new(Gate { entered, active: AtomicUsize::new(0), maximum: AtomicUsize::new(0) });
            let repo = Arc::new(Repo { discovery_gate: Mutex::new(Some(gate.clone())), ..Default::default() });
            let owner = Arc::new(RwLock::new(crate::shard::ShardOwner::new(1)));
            owner.write().unwrap().record_acquired(ShardId(0), ShardEpoch::ZERO);
            owner.write().unwrap().mark_active(ShardId(0));
            let homes = Arc::new(crate::discovery::PlacementQueueHomes { owner: owner.clone(), counts: RwLock::new((1, 1)) });
            let workflow = crate::discovery::DiscoveryRegistry::default();
            let activity = crate::activity_discovery::ActivityDiscoveryRegistry::default();
            let mut workflow_demands = Vec::new();
            let mut activity_demands = Vec::new();
            let namespace_id = NamespaceId::new();
            for number in 0..families {
                let index = if reverse { families - number - 1 } else { number };
                let queue = QueueKey { namespace_id, task_queue: TaskQueueName(format!("q-{index}")), task_kind: TaskKind::Workflow, deployment: None, build_id: None };
                let home = crate::discovery::QueueHomeProvider::local_home(homes.as_ref(), &queue).unwrap();
                workflow_demands.push(workflow.register_poll(queue.clone(), home.clone(), tokio::time::Instant::now()));
                activity_demands.push(activity.register_poll(QueueKey { task_kind: TaskKind::Activity, ..queue }, home, tokio::time::Instant::now()).unwrap());
            }
            let cancel = CancellationToken::new();
            let task = tokio::spawn(crate::discovery::run_discovery(
                crate::discovery::WorkflowSource { repo: repo.clone(), registry: Arc::new(RwLock::new(None)) }, workflow,
                InMemoryBroker::with_discovery(homes.clone()),
                crate::activity_discovery::ActivityDiscovery {
                    source: Arc::new(crate::activity_discovery::ActivitySource { repo: repo.clone(), registry: Arc::new(RwLock::new(None)), owner }),
                    registry: activity, broker: InMemoryActivityBroker::with_discovery(homes),
                    clock: tokeira_clock::system_clock(),
                }, cancel.clone(),
            ));
            let mut held = VecDeque::new();
            let mut seen = HashSet::new();
            let mut first_activity = 0;
            for _ in 0..8 {
                let (kind, queue, resume) = arrivals.recv().await.unwrap();
                first_activity += usize::from(kind == TaskKind::Activity);
                assert!(seen.insert((kind, queue)));
                held.push_back(resume);
            }
            assert_eq!(first_activity, 4);
            assert_eq!(gate.active.load(Ordering::SeqCst), 8);
            while seen.len() < 2 * families {
                held.pop_front().unwrap().send(()).unwrap();
                let (kind, queue, resume) = arrivals.recv().await.unwrap();
                assert!(seen.insert((kind, queue)), "a repeated head displaced an unserved family");
                held.push_back(resume);
            }
            assert_eq!(gate.maximum.load(Ordering::SeqCst), 8);
            cancel.cancel();
            task.await.unwrap();
            assert_eq!(gate.active.load(Ordering::SeqCst), 0);
            assert_eq!(repo.legacy_delivery_calls.load(Ordering::SeqCst), 0);
        });
    }
}
