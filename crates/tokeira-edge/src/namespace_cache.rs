use std::{collections::HashMap, sync::Arc};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use tokeira_runtime::{
    WorkerComputeCatalogError, WorkerComputeNamespace, WorkerComputeNamespaceCatalog,
};
use tokeira_types::NamespaceId;
use tokio::sync::RwLock;
use uuid::Uuid;

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

/// Runtime-facing active-namespace catalog over the edge namespace authority.
///
/// The controller receives only stable IDs and public names. Deleted tombstones and
/// edge-specific retention/visibility metadata never cross this boundary.
#[derive(Clone)]
pub struct WorkerComputeNamespaceCatalogAdapter {
    namespaces: Arc<dyn NamespaceCache>,
}

impl std::fmt::Debug for WorkerComputeNamespaceCatalogAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerComputeNamespaceCatalogAdapter")
            .finish_non_exhaustive()
    }
}

impl WorkerComputeNamespaceCatalogAdapter {
    /// Construct an adapter over the namespace cache shared by the edge.
    #[must_use]
    pub fn new(namespaces: Arc<dyn NamespaceCache>) -> Self {
        Self { namespaces }
    }
}

#[async_trait]
impl WorkerComputeNamespaceCatalog for WorkerComputeNamespaceCatalogAdapter {
    async fn list_active(&self) -> Result<Vec<WorkerComputeNamespace>, WorkerComputeCatalogError> {
        let mut namespaces = self
            .namespaces
            .list_all()
            .await
            .map_err(worker_compute_catalog_error)?
            .into_iter()
            .filter(|namespace| !namespace.deleted)
            .map(|namespace| {
                let namespace_id = namespace
                    .namespace_id
                    .as_deref()
                    .map(parse_namespace_id)
                    .transpose()?
                    .unwrap_or_else(|| {
                        crate::translate::to_internal::namespace_id_for(&namespace.name)
                    });
                Ok(WorkerComputeNamespace {
                    namespace_id,
                    name: namespace.name,
                })
            })
            .collect::<Result<Vec<_>, WorkerComputeCatalogError>>()?;
        namespaces.sort_by(|left, right| {
            left.namespace_id
                .0
                .as_bytes()
                .cmp(right.namespace_id.0.as_bytes())
        });
        Ok(namespaces)
    }

    async fn name_for_id(
        &self,
        namespace_id: NamespaceId,
    ) -> Result<Option<String>, WorkerComputeCatalogError> {
        Ok(self
            .namespaces
            .get_by_id(&namespace_id.0.to_string())
            .await
            .map_err(worker_compute_catalog_error)?
            .filter(|namespace| !namespace.deleted)
            .map(|namespace| namespace.name))
    }
}

fn parse_namespace_id(value: &str) -> Result<NamespaceId, WorkerComputeCatalogError> {
    Uuid::parse_str(value)
        .map(NamespaceId)
        .map_err(|error| WorkerComputeCatalogError {
            message: format!("invalid durable namespace ID: {error}"),
        })
}

fn worker_compute_catalog_error(error: anyhow::Error) -> WorkerComputeCatalogError {
    WorkerComputeCatalogError {
        message: error.to_string(),
    }
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

#[cfg(test)]
mod worker_compute_tests {
    use super::*;

    #[tokio::test]
    async fn worker_compute_catalog_lists_only_active_namespaces_in_id_order() {
        let cache = Arc::new(InMemoryNamespaceCache::new());
        let second = NamespaceId::new();
        let first = NamespaceId::new();
        for namespace in [
            ResolvedNamespace {
                name: "second".to_owned(),
                namespace_id: Some(second.0.to_string()),
                ..ResolvedNamespace::active("second")
            },
            ResolvedNamespace {
                name: "first".to_owned(),
                namespace_id: Some(first.0.to_string()),
                ..ResolvedNamespace::active("first")
            },
            ResolvedNamespace {
                name: "deleted".to_owned(),
                namespace_id: Some(NamespaceId::new().0.to_string()),
                deleted: true,
                ..ResolvedNamespace::active("deleted")
            },
        ] {
            cache.insert(namespace).await.expect("namespace insert");
        }
        let catalog = WorkerComputeNamespaceCatalogAdapter::new(cache);
        let listed = catalog.list_active().await.expect("active namespaces");
        assert_eq!(listed.len(), 2);
        assert!(
            listed.windows(2).all(|pair| {
                pair[0].namespace_id.0.as_bytes() < pair[1].namespace_id.0.as_bytes()
            })
        );
        assert_eq!(
            catalog.name_for_id(first).await.expect("namespace lookup"),
            Some("first".to_owned())
        );
    }
}
