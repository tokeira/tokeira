//! Memory-side adapters for the shared bounded-bulk-writes contracts.

use super::*;
use crate::bulk_write_tests::{
    Backend, OwnedRows, StoredBatch, deletion_of_a_large_run, plain_reset_of_a_closed_workflow,
    reset_over_one_mib, reset_over_ten_mib, reset_with_a_start_between, reset_with_many_timers,
    spill_of_large_entries, spill_of_many_small_entries,
};

#[async_trait]
impl Backend for InMemoryStore {
    fn repo(&self) -> &dyn RunRepository {
        self
    }

    fn shard_count(&self) -> u32 {
        1
    }

    async fn owned_rows(&self, run_key: RunKey) -> Result<OwnedRows> {
        let store = self.inner.lock().await;
        Ok(OwnedRows {
            hot: usize::from(store.runs.contains_key(&run_key)),
            history: history_rows(&store, run_key),
            request_dedupe: store
                .request_dedupe
                .values()
                .filter(|record| record.run_key == run_key)
                .count(),
            timers: store
                .timer_bucket
                .keys()
                .filter(|(candidate, _)| *candidate == run_key)
                .count(),
            activity_dispatch: store
                .activity_dispatch
                .keys()
                .filter(|(candidate, _)| *candidate == run_key)
                .count(),
            workflow_dispatch: usize::from(store.workflow_dispatch.contains_key(&run_key)),
            backlog: store
                .dispatch_backlog
                .iter()
                .filter(|entry| entry.run_key == run_key)
                .count(),
        })
    }

    async fn history_batches(&self, run_key: RunKey) -> Result<Vec<StoredBatch>> {
        let store = self.inner.lock().await;
        let batches = store
            .transition_audit
            .get(&run_key)
            .into_iter()
            .flatten()
            .filter(|record| !record.history_events.is_empty())
            .map(|record| {
                Ok(StoredBatch {
                    events: record.history_events.len(),
                    bytes: crate::codec::encode_history_events(&record.history_events)?.len(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        if !batches.is_empty() {
            return Ok(batches);
        }
        // A history without audit records is a reset copy written as one batch,
        // as both stores write it before `bounded-bulk-writes`.
        match store.history.get(&run_key) {
            Some(history) if !history.is_empty() => Ok(vec![StoredBatch {
                events: history.len(),
                bytes: crate::codec::encode_history_events(history)?.len(),
            }]),
            _ => Ok(Vec::new()),
        }
    }
}

#[tokio::test]
async fn bulk_spill_of_many_small_entries() {
    spill_of_many_small_entries(&InMemoryStore::default()).await;
}

#[tokio::test]
async fn bulk_spill_of_large_entries() {
    spill_of_large_entries(&InMemoryStore::default()).await;
}

#[tokio::test]
async fn bulk_deletion_of_a_large_run() {
    deletion_of_a_large_run(&InMemoryStore::default()).await;
}

#[tokio::test]
async fn bulk_reset_over_one_mib() {
    reset_over_one_mib(&InMemoryStore::default()).await;
}

#[tokio::test]
async fn bulk_reset_over_ten_mib() {
    reset_over_ten_mib(&InMemoryStore::default()).await;
}

#[tokio::test]
async fn bulk_reset_with_many_timers() {
    reset_with_many_timers(&InMemoryStore::default()).await;
}

#[tokio::test]
async fn bulk_plain_reset_of_a_closed_workflow() {
    plain_reset_of_a_closed_workflow(&InMemoryStore::default()).await;
}

#[tokio::test]
async fn bulk_reset_with_a_start_between() {
    reset_with_a_start_between(&InMemoryStore::default()).await;
}
