use std::{collections::HashMap, sync::Arc};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use tokio::sync::RwLock;

/// Default per-namespace workflow-execution retention, in seconds (24h). Tokeira's
/// scoped namespace model applies this when a `RegisterNamespace` request omits a
/// positive retention, and to pre-seeded namespaces, so the value is always present
/// on the wire (Temporal clients/UI treat `NamespaceConfig.workflow_execution_retention_ttl`
/// as always set).
pub const DEFAULT_NAMESPACE_RETENTION_SECONDS: i64 = 24 * 60 * 60;

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
    /// Workflow-execution retention period for this namespace, echoed on
    /// `DescribeNamespace`/`ListNamespaces`. Set from the `RegisterNamespace`
    /// request (or [`DEFAULT_NAMESPACE_RETENTION_SECONDS`] when omitted).
    pub retention: time::Duration,
}

impl ResolvedNamespace {
    pub fn active(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            namespace_id: None,
            is_global: false,
            visibility_enabled: true,
            deleted: false,
            retention: time::Duration::seconds(DEFAULT_NAMESPACE_RETENTION_SECONDS),
        }
    }
}

#[async_trait]
pub trait NamespaceCache: Send + Sync + 'static {
    /// Resolve a namespace by its current name.
    async fn get(&self, name: &str) -> Result<Option<ResolvedNamespace>>;

    /// Resolve a namespace by stable ID, including a deleted-name tombstone.
    ///
    /// The default scans the small edge registry and derives IDs for legacy entries
    /// that predate explicit `namespace_id` population.
    async fn get_by_id(&self, namespace_id: &str) -> Result<Option<ResolvedNamespace>> {
        Ok(self.list_all().await?.into_iter().find(|namespace| {
            namespace.namespace_id.as_deref() == Some(namespace_id)
                || crate::translate::to_internal::namespace_id_for(&namespace.name)
                    .0
                    .to_string()
                    == namespace_id
        }))
    }

    /// Return every namespace record, including deleted-name tombstones.
    async fn list_all(&self) -> Result<Vec<ResolvedNamespace>>;

    /// Insert or replace a namespace record under its current name.
    async fn insert(&self, ns: ResolvedNamespace) -> Result<()>;

    /// Atomically replace `current_name` with `namespace` under its new name.
    ///
    /// Namespace deletion depends on removing the live name and publishing the
    /// deleted-name tombstone as one registry mutation; a default error keeps test
    /// doubles source-compatible without pretending they support that guarantee.
    async fn replace_name(&self, current_name: &str, namespace: ResolvedNamespace) -> Result<()> {
        let _ = (current_name, namespace);
        Err(anyhow!("atomic namespace rename is not supported"))
    }

    /// Remove and return a namespace record by its current name.
    async fn remove(&self, name: &str) -> Result<Option<ResolvedNamespace>> {
        let _ = name;
        Err(anyhow!("namespace removal is not supported"))
    }
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

    async fn replace_name(&self, current_name: &str, namespace: ResolvedNamespace) -> Result<()> {
        let mut inner = self.inner.write().await;
        inner.remove(current_name);
        inner.insert(namespace.name.clone(), namespace);
        Ok(())
    }

    async fn remove(&self, name: &str) -> Result<Option<ResolvedNamespace>> {
        Ok(self.inner.write().await.remove(name))
    }
}
