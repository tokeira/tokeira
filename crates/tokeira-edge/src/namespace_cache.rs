use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::RwLock;

/// Namespace metadata resolved at the edge.
///
/// The runtime should not need to rediscover obvious namespace facts such as
/// "does this namespace exist?" or "has it been deleted?". Rejecting those cases
/// early saves work and keeps authz decisions namespace-aware.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedNamespace {
    pub name: String,
    pub namespace_id: Option<String>,
    pub is_global: bool,
    pub visibility_enabled: bool,
    pub deleted: bool,
}

impl ResolvedNamespace {
    pub fn active(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            namespace_id: None,
            is_global: false,
            visibility_enabled: true,
            deleted: false,
        }
    }
}

#[async_trait]
pub trait NamespaceCache: Send + Sync + 'static {
    async fn get(&self, name: &str) -> Result<Option<ResolvedNamespace>>;
    async fn list_all(&self) -> Result<Vec<ResolvedNamespace>>;
    async fn insert(&self, ns: ResolvedNamespace) -> Result<()>;
}

/// Simple in-memory cache useful for tests and local bring-up.
///
/// A real deployment will usually back this by runtime/storage metadata and
/// refresh it opportunistically. The edge does not need to know *how* refresh
/// happens; it only needs a namespace lookup abstraction.
#[derive(Debug, Default)]
pub struct InMemoryNamespaceCache {
    inner: Arc<RwLock<HashMap<String, ResolvedNamespace>>>,
}

impl InMemoryNamespaceCache {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl NamespaceCache for InMemoryNamespaceCache {
    async fn get(&self, name: &str) -> Result<Option<ResolvedNamespace>> {
        Ok(self.inner.read().await.get(name).cloned())
    }

    async fn list_all(&self) -> Result<Vec<ResolvedNamespace>> {
        Ok(self.inner.read().await.values().cloned().collect())
    }

    async fn insert(&self, ns: ResolvedNamespace) -> Result<()> {
        self.inner.write().await.insert(ns.name.clone(), ns);
        Ok(())
    }
}
