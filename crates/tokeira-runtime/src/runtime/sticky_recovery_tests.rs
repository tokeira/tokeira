//! End-to-end recovery without publication, normal demand, or timeout progress.

use super::*;
use proptest::prelude::*;
use tokeira_storage::{InMemoryStore, WriteFence};
use tokeira_types::StickyAffinity;

#[derive(Debug)]
struct RecoveryClock(OffsetDateTime, tokio::time::Instant);
impl Clock for RecoveryClock {
    fn now(&self) -> OffsetDateTime {
        self.0 + Duration::try_from(self.1.elapsed()).unwrap()
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: queue-home-delivery, Property 1: lost sticky publication is recovered before its timeout, without normal discovery demand.
    #[test]
    fn property_1_lost_sticky_row_recovered_before_deadline(index in 0usize..100, seeded in any::<bool>()) {
        tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap().block_on(async {
            let repo = Arc::new(InMemoryStore::default());
            let clock: Arc<dyn Clock> = Arc::new(RecoveryClock(OffsetDateTime::now_utc(), tokio::time::Instant::now()));
            let runtime = TokeiraRuntime::new_with_nexus_and_shards_and_endpoint(
                repo.clone(), 1, LaneConfig::default(), TimerScannerConfig::default(),
                WorkflowTimeoutScannerConfig::default(), ActivityTimeoutScannerConfig::default(),
                NexusTimeoutScannerConfig::default(), NexusEndpointRegistry::default(),
                Arc::new(NoopNexusHttpClient), NexusCompletionDeps::default(), 1,
                "sticky-recovery".into(), "127.0.0.1:0".into(), seeded, None, clock.clone(),
            );
            if !seeded { runtime.recover_self_assigned_shard(ShardId(0), ShardEpoch::ZERO).await.unwrap(); }
            let normal = QueueKey { namespace_id: NamespaceId::new(), task_queue: TaskQueueName(format!("normal-{index}")), task_kind: TaskKind::Workflow, deployment: None, build_id: None };
            let mut item = crate::discovery::tests::transition(&normal, index);
            let state = &mut item.next_state;
            state.sticky = Some(StickyAffinity { sticky_queue: TaskQueueName("lost-sticky".into()), worker_identity: WorkerIdentity("absent".into()), schedule_to_start_timeout: Duration::seconds(30) });
            let pending = state.pending_workflow_task.as_mut().unwrap();
            pending.scheduled_at = clock.now();
            pending.schedule_to_start_deadline = Some(clock.now() + Duration::seconds(30));
            let key = state.run_key;
            repo.commit_transition(key, item, WriteFence::Unfenced).await.unwrap();
            let before = repo.snapshot().await.unwrap();
            let found = runtime.broker.poll_workflow_task(&normal, &WorkerIdentity("normal-worker".into()), std::time::Duration::from_secs(3)).await.unwrap();
            prop_assert!(found.is_some(), "lost sticky row must fall back before its 30-second deadline");
            prop_assert!(matches!(found.unwrap(), crate::broker::WorkflowPollResult::Queued(task, _) if task.run_key == key));
            prop_assert_eq!(repo.snapshot().await.unwrap(), before);
            prop_assert_eq!(runtime.discovery.0.registered_ranges(), 0);
            discovery_tests::stop(&runtime).await;
            Ok(())
        })?;
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: queue-home-delivery, Property 8: neither seeded nor explicit acquisition starts sticky reads until repair and activation complete.
    #[test]
    fn property_8_sticky_sweep_waits_for_repair(seeded in any::<bool>(), interrupted in any::<bool>(), index in 0usize..100) {
        tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap().block_on(async {
            let repo = Arc::new(repair_tests::Repo::default());
            let normal = QueueKey { namespace_id: NamespaceId::new(), task_queue: TaskQueueName("repair-gate".into()), task_kind: TaskKind::Workflow, deployment: None, build_id: None };
            let item = crate::discovery::tests::transition(&normal, index);
            repo.inner.commit_transition(item.next_state.run_key, item, WriteFence::Unfenced).await.unwrap();
            let (entered, resume) = repo.pause(repair_tests::Point::Home);
            let (queries, mut arrivals) = tokio::sync::mpsc::unbounded_channel();
            *repo.sticky_gate.lock().unwrap() = Some(Arc::new(repair_tests::StickyPageGate { entered: queries, active: Default::default() }));
            let runtime = Arc::new(TokeiraRuntime::new_with_nexus_and_shards_and_endpoint(
                repo.clone(), 1, LaneConfig::default(), TimerScannerConfig::default(),
                WorkflowTimeoutScannerConfig::default(), ActivityTimeoutScannerConfig::default(),
                NexusTimeoutScannerConfig::default(), NexusEndpointRegistry::default(),
                Arc::new(NoopNexusHttpClient), NexusCompletionDeps::default(), 1,
                "sticky-recovery-gate".into(), "127.0.0.1:0".into(), seeded, None, tokeira_clock::system_clock(),
            ));
            let acquiring = runtime.clone();
            let recovery = tokio::spawn(async move {
                if seeded { acquiring.ready().await }
                else { acquiring.recover_self_assigned_shard(ShardId(0), ShardEpoch::ZERO).await.map(|_| ()) }
            });
            entered.await.unwrap();
            prop_assert!(runtime.active_shards().is_empty());
            prop_assert!(arrivals.try_recv().is_err());
            if interrupted { drop(resume); } else { resume.send(()).unwrap(); }
            prop_assert_eq!(recovery.await.unwrap().is_err(), interrupted);
            if interrupted {
                prop_assert!(runtime.active_shards().is_empty());
                prop_assert!(arrivals.try_recv().is_err());
            } else {
                let (home, cursor, limit, _, _) = arrivals.recv().await.unwrap();
                prop_assert_eq!(home, ShardId(0)); prop_assert_eq!(cursor, None); prop_assert_eq!(limit, 64);
                prop_assert_eq!(runtime.active_shards(), vec![ShardId(0)]);
            }
            discovery_tests::stop(&runtime).await;
            Ok(())
        })?;
    }
}
