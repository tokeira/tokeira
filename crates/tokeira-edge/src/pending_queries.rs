//! Poll-to-completion bridge for in-flight query channels.
//!
//! Bind `(task_token, query_id)` to the original caller so workflow and legacy
//! query replies reach the correct waiter. Direct queries park their sender
//! here. Forwarded buffered queries retain their sender and payload in
//! [`BufferedQueryRegistry`] until answered, so a lost start reply cannot
//! consume the only copy. A replacement attempt retires the old token binding.
//! The registry decides barrier eligibility; this store binds an eligible
//! selection to the task that may answer it.

use std::{collections::HashMap, sync::Arc};

use tokeira_runtime::{BufferedQueryRegistry, QueryResult};
use tokeira_types::RunKey;
use tokio::sync::{Mutex, oneshot};

pub const LEGACY_QUERY_ID: &str = "__legacy__";

#[derive(Clone, Default, Debug)]
pub struct PendingQueryStore {
    inner: Arc<Mutex<PendingQueries>>,
}

#[derive(Debug, Default)]
struct PendingQueries {
    tasks: HashMap<Vec<u8>, HashMap<String, QueryWaiter>>,
    bindings: HashMap<(RunKey, String), Vec<u8>>,
}

#[derive(Debug)]
enum QueryWaiter {
    Direct(oneshot::Sender<QueryResult>),
    Buffered {
        registry: BufferedQueryRegistry,
        run: RunKey,
        id: String,
    },
}

impl QueryWaiter {
    fn take(self) -> Option<oneshot::Sender<QueryResult>> {
        match self {
            Self::Direct(sender) => Some(sender),
            Self::Buffered { registry, run, id } => {
                registry.remove(run, &id).map(|query| query.response_tx)
            }
        }
    }
}

impl PendingQueryStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn insert(&self, token: &[u8], query_id: String, tx: oneshot::Sender<QueryResult>) {
        self.inner
            .lock()
            .await
            .tasks
            .entry(token.to_vec())
            .or_default()
            .insert(query_id, QueryWaiter::Direct(tx));
    }

    /// Bind an attempt to a buffered query without consuming its payload or
    /// waiter. A lost task reply can then be retried with the same query ID.
    pub async fn insert_buffered(
        &self,
        token: &[u8],
        query_id: String,
        registry: BufferedQueryRegistry,
        run: RunKey,
    ) {
        let mut inner = self.inner.lock().await;
        // Only the current attempt needs a token binding. A late reply for a
        // retired attempt must not consume the current buffered waiter.
        if let Some(old) = inner
            .bindings
            .insert((run, query_id.clone()), token.to_vec())
            && let Some(queries) = inner.tasks.get_mut(&old)
        {
            queries.remove(&query_id);
            if queries.is_empty() {
                inner.tasks.remove(&old);
            }
        }
        inner.tasks.entry(token.to_vec()).or_default().insert(
            query_id.clone(),
            QueryWaiter::Buffered {
                registry,
                run,
                id: query_id,
            },
        );
    }

    pub async fn take(&self, token: &[u8], query_id: &str) -> Option<oneshot::Sender<QueryResult>> {
        let mut inner = self.inner.lock().await;
        let queries = inner.tasks.get_mut(token)?;
        let sender = queries.remove(query_id);
        if queries.is_empty() {
            inner.tasks.remove(token);
        }
        if let Some(QueryWaiter::Buffered { run, id, .. }) = &sender {
            inner.bindings.remove(&(*run, id.clone()));
        }
        sender.and_then(QueryWaiter::take)
    }

    pub async fn drain(&self, token: &[u8]) -> Vec<(String, oneshot::Sender<QueryResult>)> {
        let mut inner = self.inner.lock().await;
        let queries = inner.tasks.remove(token).unwrap_or_default();
        queries
            .into_iter()
            .filter_map(|(id, waiter)| {
                if let QueryWaiter::Buffered { run, id, .. } = &waiter {
                    inner.bindings.remove(&(*run, id.clone()));
                }
                waiter.take().map(|sender| (id, sender))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokeira_runtime::BufferedQuery;
    use tokeira_types::Payloads;

    #[tokio::test]
    async fn lost_reply_retains_query_and_replacement_attempt_retires_old_binding() {
        let store = PendingQueryStore::new();
        let registry = BufferedQueryRegistry::default();
        let run = RunKey::new();
        let (tx, rx) = oneshot::channel();
        registry
            .buffer(
                run,
                BufferedQuery {
                    query_id: "query".into(),
                    query_type: "read".into(),
                    query_args: Payloads::default(),
                    required_barrier: 4,
                    enqueued_at: std::time::Instant::now(),
                    deadline: time::OffsetDateTime::now_utc() + time::Duration::minutes(1),
                    response_tx: tx,
                },
            )
            .unwrap();
        store
            .insert_buffered(b"lost", "query".into(), registry.clone(), run)
            .await;
        assert_eq!(registry.snapshot(run).len(), 1);
        store
            .insert_buffered(b"replacement", "query".into(), registry.clone(), run)
            .await;
        assert!(store.take(b"lost", "query").await.is_none());
        let sender = store.take(b"replacement", "query").await.unwrap();
        assert!(!registry.has_buffered(run));
        sender
            .send(QueryResult::Failed {
                message: "worker result".into(),
                failure: None,
            })
            .unwrap();
        assert!(
            matches!(rx.await.unwrap(), QueryResult::Failed { message, .. } if message == "worker result")
        );
        assert!(store.inner.lock().await.bindings.is_empty());
    }
}
