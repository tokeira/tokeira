//! Run-local buffering for consistent queries.
//!
//! Query correctness is enforced here, not in the broker. A query is attached
//! to a run-local barrier representing the last write the caller must observe;
//! it only becomes eligible for delivery once the runtime can prove that the
//! run has advanced far enough. This avoids the older transport-level failure
//! mode where queries could sit beside workflow tasks and observe stale state.
//!
//! The barrier-release condition: a buffered query with `required_barrier = B`
//! is released when the transport calls `drain_satisfied(run_key, observable)`
//! with `observable >= B`. In practice this happens after a WFT completion
//! commits and the run's `last_event_id` advances past the barrier. Queries
//! whose barrier is still ahead of the observable watermark stay buffered
//! until the next completion cycle.

use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::Instant,
};

use tokeira_types::{Payloads, RunKey};
use tokio::sync::oneshot;

use crate::QueryResult;
use tokeira_observability::QueryBufferOutcomeLabel;

const MAX_BUFFERED_QUERIES_PER_RUN: usize = 256;

/// Buffered query plus the consistency barrier it must observe before a worker
/// is allowed to evaluate it.
#[derive(Debug)]
pub struct BufferedQuery {
    pub query_id: String,
    pub query_type: String,
    pub query_args: Payloads,
    pub required_barrier: i64,
    pub enqueued_at: Instant,
    pub response_tx: oneshot::Sender<QueryResult>,
}

#[derive(Clone, Debug, Default)]
pub struct BufferedQueryRegistry {
    inner: Arc<Mutex<HashMap<RunKey, VecDeque<BufferedQuery>>>>,
}

impl BufferedQueryRegistry {
    /// Buffer a query behind its required barrier.
    ///
    /// Capacity is per run so one hot execution cannot consume unbounded memory
    /// and starve unrelated workflows.
    pub fn buffer(&self, run_key: RunKey, query: BufferedQuery) -> Result<(), BufferedQuery> {
        let mut inner = self.inner.lock().expect("buffered query registry poisoned");
        let queries = inner.entry(run_key).or_default();
        if queries.len() >= MAX_BUFFERED_QUERIES_PER_RUN {
            return Err(query);
        }
        queries.push_back(query);
        Ok(())
    }

    pub fn drain_satisfied(&self, run_key: RunKey, observable_barrier: i64) -> Vec<BufferedQuery> {
        let mut inner = self.inner.lock().expect("buffered query registry poisoned");
        let Some(queries) = inner.get_mut(&run_key) else {
            return Vec::new();
        };

        let mut drained = Vec::new();
        let mut remaining = VecDeque::with_capacity(queries.len());
        while let Some(query) = queries.pop_front() {
            if query.required_barrier <= observable_barrier {
                crate::metrics::record_query_buffer_wait(
                    query.enqueued_at.elapsed(),
                    QueryBufferOutcomeLabel::Released,
                );
                drained.push(query);
            } else {
                remaining.push_back(query);
            }
        }

        if remaining.is_empty() {
            inner.remove(&run_key);
        } else {
            *queries = remaining;
        }
        drained
    }

    pub fn drain_all(&self, run_key: RunKey) -> Vec<BufferedQuery> {
        self.inner
            .lock()
            .expect("buffered query registry poisoned")
            .remove(&run_key)
            .map(|queries| queries.into_iter().collect())
            .unwrap_or_default()
    }

    pub fn remove(&self, run_key: RunKey, query_id: &str) -> Option<BufferedQuery> {
        let mut inner = self.inner.lock().expect("buffered query registry poisoned");
        let queries = inner.get_mut(&run_key)?;
        let idx = queries
            .iter()
            .position(|query| query.query_id == query_id)?;
        let removed = queries.remove(idx);
        if queries.is_empty() {
            inner.remove(&run_key);
        }
        removed
    }

    pub fn has_buffered(&self, run_key: RunKey) -> bool {
        self.inner
            .lock()
            .expect("buffered query registry poisoned")
            .get(&run_key)
            .is_some_and(|queries| !queries.is_empty())
    }

    pub fn fail_run_queries(&self, run_key: RunKey, message: impl Into<String>) {
        let message = message.into();
        for query in self.drain_all(run_key) {
            crate::metrics::record_query_buffer_wait(
                query.enqueued_at.elapsed(),
                QueryBufferOutcomeLabel::Failed,
            );
            let _ = query.response_tx.send(QueryResult::Failed {
                message: message.clone(),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query(id: usize, barrier: i64) -> BufferedQuery {
        let (tx, _rx) = oneshot::channel();
        BufferedQuery {
            query_id: format!("q-{id}"),
            query_type: "query".to_string(),
            query_args: Payloads::default(),
            required_barrier: barrier,
            enqueued_at: Instant::now(),
            response_tx: tx,
        }
    }

    #[test]
    fn drain_satisfied_only_returns_queries_at_or_below_barrier() {
        let registry = BufferedQueryRegistry::default();
        let run_key = RunKey::new();
        registry.buffer(run_key, query(1, 1)).unwrap();
        registry.buffer(run_key, query(2, 3)).unwrap();
        registry.buffer(run_key, query(3, 2)).unwrap();

        let drained = registry.drain_satisfied(run_key, 2);
        let ids: Vec<_> = drained.into_iter().map(|query| query.query_id).collect();
        assert_eq!(ids, vec!["q-1".to_string(), "q-3".to_string()]);
        assert!(registry.has_buffered(run_key));

        let remaining = registry.drain_satisfied(run_key, 10);
        assert_eq!(remaining.len(), 1);
        assert!(!registry.has_buffered(run_key));
    }

    #[test]
    fn rejects_queries_over_per_run_limit() {
        let registry = BufferedQueryRegistry::default();
        let run_key = RunKey::new();
        for id in 0..MAX_BUFFERED_QUERIES_PER_RUN {
            registry.buffer(run_key, query(id, 0)).unwrap();
        }
        assert!(
            registry
                .buffer(run_key, query(MAX_BUFFERED_QUERIES_PER_RUN, 0))
                .is_err()
        );
    }
}
