//! Generated finite scheduler traces against the production queue and attempts.

use super::*;
use crate::workflow_delivery::tests::{clock, node, queue, runtime, stored};
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use proptest::prelude::*;
use tokeira_storage::InMemoryStore;
use tokio::sync::mpsc;

fn enqueue(
    sender: &WorkflowOfferSender,
    offer: WorkflowOffer,
    acquisition: &ShardAcquisition,
    source: PublicationSource,
    destination: ShardId,
) -> EnqueueOutcome {
    sender.try_enqueue(
        offer,
        acquisition.clone(),
        source,
        destination,
        Instant::now(),
        false,
        time::OffsetDateTime::UNIX_EPOCH,
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: queue-home-delivery, Property 7: retained count/bytes include active work; coalescing never renews or grows it.
    #[test]
    fn property_7_sender_retention_bounds(index in 0u32..100, byte_bound in any::<bool>()) {
        runtime().block_on(async {
            let repo = Arc::new(InMemoryStore::with_shard_count(8)); let clock = clock();
            let mut offer = stored(&repo, &queue(index), None, index, clock.as_ref()).await;
            let delivery = node(repo, &[offer.hint.execution_home], clock);
            let acquisition = delivery.acquisition(offer.hint.execution_home).unwrap();
            if byte_bound { offer.directive.workflow_id.0 = "x".repeat(32 * 1024); }
            let sender = WorkflowOfferSender::default(); let mut expected_bytes = 0;
            let mut count = 0;
            loop {
                offer.hint.task.run_key = RunKey(uuid::Uuid::from_u128(count as u128 + 1));
                let bytes = crate::workflow_offers::serialized_hint_bytes(&offer, BYTES);
                let outcome = enqueue(&sender, offer.clone(), &acquisition, PublicationSource::Publication, ShardId(7));
                if outcome == EnqueueOutcome::Refused {
                    prop_assert!(count == ENTRIES || expected_bytes + bytes > BYTES); break;
                }
                count += 1; expected_bytes += bytes;
                prop_assert_eq!(outcome, EnqueueOutcome::Queued);
                prop_assert_eq!(enqueue(&sender, offer.clone(), &acquisition, PublicationSource::Recovery, ShardId(6)), EnqueueOutcome::Coalesced);
                let state = sender.state.lock().unwrap(); prop_assert_eq!(state.bytes, expected_bytes); prop_assert_eq!(state.keys.len(), count);
            }
            if byte_bound { prop_assert!(count < ENTRIES); } else { prop_assert_eq!(count, ENTRIES); }
            acquisition.cancel.cancel();
            sender.state.lock().unwrap().prune(Instant::now());
            prop_assert_eq!(sender.state.lock().unwrap().bytes, 0);
            prop_assert!(sender.state.lock().unwrap().keys.is_empty());
            Ok(())
        })?;
    }

    // Feature: queue-home-delivery, Property 7: alternating sources and round-robin homes enforce node and destination caps.
    #[test]
    fn property_7_sender_fairness_and_concurrency(index in 0u32..100, home_count in 4u32..9, rounds in 1usize..5) {
        runtime().block_on(async {
            let repo = Arc::new(InMemoryStore::with_shard_count(8)); let clock = clock();
            let mut offer = stored(&repo, &queue(index), None, index, clock.as_ref()).await;
            let delivery = node(repo, &[offer.hint.execution_home], clock); let acquisition = delivery.acquisition(offer.hint.execution_home).unwrap();
            let sender = WorkflowOfferSender::default(); let mut key = 1u128;
            for source in [PublicationSource::Publication, PublicationSource::Recovery] { for home in 0..home_count { for _ in 0..20 {
                offer.hint.task.run_key = RunKey(uuid::Uuid::from_u128(key)); key += 1;
                prop_assert_eq!(enqueue(&sender, offer.clone(), &acquisition, source, ShardId(home)), EnqueueOutcome::Queued);
            }}}
            let mut state = sender.state.lock().unwrap();
            for _ in 0..rounds {
                let mut active = Vec::new();
                for slot in 0..CONCURRENCY {
                    let work = state.take(Instant::now()).unwrap();
                    prop_assert_eq!(work.source, if slot % 2 == 0 { PublicationSource::Publication } else { PublicationSource::Recovery });
                    prop_assert!(state.active.values().all(|count| *count <= PER_DESTINATION));
                    active.push(work);
                }
                prop_assert!(state.take(Instant::now()).is_none());
                prop_assert_eq!(state.active.values().sum::<usize>(), CONCURRENCY);
                prop_assert_eq!(state.active.len(), home_count as usize);
                for work in active { *state.active.get_mut(&work.destination).unwrap() -= 1; state.release(&work); }
                state.active.clear();
            }
            Ok(())
        })?;
    }
}

#[derive(Debug)]
struct ScriptSink {
    calls: Mutex<Vec<bool>>,
    mode: u8,
}
#[async_trait::async_trait]
impl RemoteWorkflowOfferSink for ScriptSink {
    async fn offer(
        &self,
        _: &WorkflowOffer,
        _: ShardId,
        refresh: bool,
    ) -> Result<WorkflowOfferOutcome, WorkflowOfferError> {
        self.calls.lock().unwrap().push(refresh);
        match self.mode {
            0 => Ok(WorkflowOfferOutcome::NotHome),
            1 => std::future::pending().await,
            2 => Err(WorkflowOfferError::Authentication),
            3 => Ok(WorkflowOfferOutcome::Full),
            _ => Err(WorkflowOfferError::Transport),
        }
    }
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: queue-home-delivery, Property 7: every attempt includes routing/credentials and all retries share one absolute second.
    #[test]
    fn property_7_sender_budgets_refresh_and_failures(index in 0u32..100, mode in 0u8..5, residence in 0u64..900) {
        runtime().block_on(async {
            let repo = Arc::new(InMemoryStore::with_shard_count(8)); let clock = clock();
            let offer = stored(&repo, &queue(index), None, index, clock.as_ref()).await;
            let execution = (0..8).map(ShardId).find(|id| *id != crate::workflow_delivery::tests::home(&offer.hint.task.queue)).unwrap();
            let delivery = node(repo, &[execution], clock); let acquisition = delivery.acquisition(execution).unwrap();
            let sender = WorkflowOfferSender::default(); let started = Instant::now();
            enqueue(&sender, offer.clone(), &acquisition, PublicationSource::Publication, ShardId(7));
            tokio::time::advance(Duration::from_millis(residence)).await;
            let mut work = sender.state.lock().unwrap().take(Instant::now()).unwrap();
            let sink = Arc::new(ScriptSink { calls: Mutex::new(Vec::new()), mode });
            loop {
                let attempt_start = Instant::now();
                let (next, _, retry) = attempt(work, delivery.clone(), sink.clone()).await; work = next;
                prop_assert!(attempt_start.elapsed() <= ATTEMPT);
                prop_assert!(started.elapsed() <= TOTAL);
                if !retry || work.deadline <= work.ready_at { break; }
                tokio::time::advance(work.ready_at - Instant::now()).await;
            }
            let calls = sink.calls.lock().unwrap();
            prop_assert!(calls.len() <= MAX_ATTEMPTS as usize);
            prop_assert!(calls.iter().filter(|refresh| **refresh).count() <= 1);
            if mode == 0 && residence < 800 { prop_assert_eq!(&*calls, &[false, true]); }
            if mode == 2 || mode == 3 { prop_assert_eq!(calls.len(), 1); }
            if mode == 1 && residence == 0 { prop_assert!(started.elapsed() >= ATTEMPT); }
            Ok(())
        })?;
    }
}

#[derive(Debug)]
struct BlockedSink {
    entered: mpsc::UnboundedSender<()>,
    dropped: mpsc::UnboundedSender<()>,
}
struct Active(mpsc::UnboundedSender<()>);
impl Drop for Active {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}
#[async_trait::async_trait]
impl RemoteWorkflowOfferSink for BlockedSink {
    async fn offer(
        &self,
        _: &WorkflowOffer,
        _: ShardId,
        _: bool,
    ) -> Result<WorkflowOfferOutcome, WorkflowOfferError> {
        let _active = Active(self.dropped.clone());
        self.entered.send(()).unwrap();
        std::future::pending().await
    }
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: queue-home-delivery, Property 7: acquisition loss releases queued and blocked work without a retry/discovery tick.
    #[test]
    fn property_7_sender_acquisition_loss(index in 0u32..100, by_deadline in any::<bool>()) {
        runtime().block_on(async {
            let repo = Arc::new(InMemoryStore::with_shard_count(8)); let clock = clock();
            let mut offer = stored(&repo, &queue(index), None, index, clock.as_ref()).await;
            let execution = (0..8).map(ShardId).find(|id| *id != crate::workflow_delivery::tests::home(&offer.hint.task.queue)).unwrap();
            let delivery = node(repo, &[execution], clock); let acquisition = delivery.acquisition(execution).unwrap();
            *acquisition.deadline.lock().unwrap() = Some(Instant::now() + Duration::from_millis(100));
            let (entered, mut entries) = mpsc::unbounded_channel(); let (dropped, mut drops) = mpsc::unbounded_channel();
            let sender = WorkflowOfferSender::spawn(delivery.clone(), Arc::new(BlockedSink { entered, dropped }));
            for key in 1..65 { offer.hint.task.run_key = RunKey(uuid::Uuid::from_u128(key)); enqueue(&sender, offer.clone(), &acquisition, PublicationSource::Publication, ShardId(7)); }
            for _ in 0..PER_DESTINATION { entries.recv().await.unwrap(); }
            prop_assert!(entries.try_recv().is_err());
            if by_deadline { tokio::time::advance(Duration::from_millis(100)).await; } else { acquisition.cancel.cancel(); }
            for _ in 0..PER_DESTINATION { drops.recv().await.unwrap(); }
            loop {
                let changed = sender.changed.notified(); tokio::pin!(changed); changed.as_mut().enable();
                if sender.state.lock().unwrap().keys.is_empty() { break; }
                changed.await;
            }
            prop_assert_eq!(sender.state.lock().unwrap().bytes, 0);
            prop_assert!(entries.try_recv().is_err());
            delivery.shutdown.begin_shutdown();
            Ok(())
        })?;
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: queue-home-delivery, Property 7: a superseded incarnation ends fallback as obsolete without an RPC or durable write.
    #[test]
    fn property_7_obsolete_fallback_outcome(index in 0u32..100) {
        let recorder = DebuggingRecorder::new();
        metrics::with_local_recorder(&recorder, || runtime().block_on(async {
            let repo = Arc::new(InMemoryStore::with_shard_count(8)); let clock = clock();
            let mut offer = stored(&repo, &queue(index), Some(&queue(index + 100)), index, clock.as_ref()).await;
            offer.hint.task.logical_seq = LogicalTaskSeq(offer.hint.task.logical_seq.0 + 1);
            let delivery = node(repo.clone(), &[offer.hint.execution_home], clock);
            let acquisition = delivery.acquisition(offer.hint.execution_home).unwrap();
            let before = repo.snapshot().await.unwrap();
            let sender = WorkflowOfferSender::default();
            sender.try_enqueue(offer, acquisition, PublicationSource::Publication, ShardId(7), Instant::now(), true, time::OffsetDateTime::UNIX_EPOCH);
            let work = sender.state.lock().unwrap().take(Instant::now()).unwrap();
            let sink = Arc::new(ScriptSink { calls: Mutex::new(Vec::new()), mode: 0 });
            let (_, _, retry) = attempt(work, delivery, sink.clone()).await;
            prop_assert!(!retry);
            prop_assert!(sink.calls.lock().unwrap().is_empty());
            prop_assert_eq!(repo.snapshot().await.unwrap(), before);
            let outcomes = recorder.snapshotter().snapshot().into_vec();
            prop_assert!(outcomes.iter().any(|(key, _, _, value)| key.key().name() == crate::metrics::WORKFLOW_OFFER_DELIVERY_TOTAL && key.key().labels().any(|label| label.key() == "outcome" && label.value() == "obsolete") && *value == DebugValue::Counter(1)));
            prop_assert!(!outcomes.iter().any(|(key, _, _, _)| key.key().labels().any(|label| label.key() == "outcome" && label.value() == "expired")));
            Ok(())
        }))?;
    }
}
