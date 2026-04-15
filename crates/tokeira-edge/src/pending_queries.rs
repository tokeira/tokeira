use std::{collections::HashMap, sync::Arc};

use tokeira_runtime::QueryResult;
use tokio::sync::{Mutex, oneshot};

pub const LEGACY_QUERY_ID: &str = "__legacy__";

#[derive(Clone, Default)]
pub struct PendingQueryStore {
    inner: Arc<Mutex<HashMap<Vec<u8>, HashMap<String, oneshot::Sender<QueryResult>>>>>,
}

impl PendingQueryStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn insert(
        &self,
        token: &[u8],
        query_id: String,
        tx: oneshot::Sender<QueryResult>,
    ) {
        self.inner
            .lock()
            .await
            .entry(token.to_vec())
            .or_default()
            .insert(query_id, tx);
    }

    pub async fn take(
        &self,
        token: &[u8],
        query_id: &str,
    ) -> Option<oneshot::Sender<QueryResult>> {
        let mut inner = self.inner.lock().await;
        let queries = inner.get_mut(token)?;
        let sender = queries.remove(query_id);
        if queries.is_empty() {
            inner.remove(token);
        }
        sender
    }

    pub async fn drain(
        &self,
        token: &[u8],
    ) -> Vec<(String, oneshot::Sender<QueryResult>)> {
        self.inner
            .lock()
            .await
            .remove(token)
            .map(|queries| queries.into_iter().collect())
            .unwrap_or_default()
    }
}
