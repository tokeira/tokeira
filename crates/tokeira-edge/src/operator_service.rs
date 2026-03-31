use std::{collections::BTreeMap, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use http::HeaderMap;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{
    errors::{EdgeError, EdgeResult},
    interceptors::{Action, EdgeInterceptors},
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchAttributeDefinition {
    pub name: String,
    pub attr_type: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterInfo {
    pub cluster_name: String,
    pub version: String,
    pub notes: Vec<String>,
}

#[async_trait]
pub trait OperatorApi: Send + Sync + 'static {
    async fn cluster_info(&self) -> Result<ClusterInfo>;

    async fn list_search_attributes(
        &self,
        namespace: Option<&str>,
    ) -> Result<Vec<SearchAttributeDefinition>>;

    async fn upsert_search_attribute(
        &self,
        namespace: &str,
        attr: SearchAttributeDefinition,
    ) -> Result<()>;

    async fn remove_search_attribute(&self, namespace: &str, attr_name: &str) -> Result<()>;
}

/// A compact in-memory implementation that makes the service easy to exercise in tests.
#[derive(Debug, Default)]
pub struct InMemoryOperatorApi {
    cluster_info: RwLock<ClusterInfo>,
    attrs: RwLock<BTreeMap<String, BTreeMap<String, SearchAttributeDefinition>>>,
}

impl InMemoryOperatorApi {
    pub fn new(cluster_name: impl Into<String>) -> Self {
        Self {
            cluster_info: RwLock::new(ClusterInfo {
                cluster_name: cluster_name.into(),
                version: "0.1.0-dev".to_string(),
                notes: vec!["in-memory operator api".to_string()],
            }),
            attrs: RwLock::new(BTreeMap::new()),
        }
    }
}

#[async_trait]
impl OperatorApi for InMemoryOperatorApi {
    async fn cluster_info(&self) -> Result<ClusterInfo> {
        Ok(self.cluster_info.read().await.clone())
    }

    async fn list_search_attributes(
        &self,
        namespace: Option<&str>,
    ) -> Result<Vec<SearchAttributeDefinition>> {
        let attrs = self.attrs.read().await;
        let values = match namespace {
            Some(ns) => attrs
                .get(ns)
                .into_iter()
                .flat_map(|m| m.values().cloned())
                .collect(),
            None => attrs
                .values()
                .flat_map(|m| m.values().cloned())
                .collect(),
        };
        Ok(values)
    }

    async fn upsert_search_attribute(
        &self,
        namespace: &str,
        attr: SearchAttributeDefinition,
    ) -> Result<()> {
        self.attrs
            .write()
            .await
            .entry(namespace.to_string())
            .or_default()
            .insert(attr.name.clone(), attr);
        Ok(())
    }

    async fn remove_search_attribute(&self, namespace: &str, attr_name: &str) -> Result<()> {
        if let Some(attrs) = self.attrs.write().await.get_mut(namespace) {
            attrs.remove(attr_name);
        }
        Ok(())
    }
}

/// Operator-facing compatibility shell.
///
/// This service should stay boring: authorization, validation, and delegation to
/// the operator/admin plane. The actual side effects belong elsewhere.
#[derive(Clone)]
pub struct OperatorService {
    api: Arc<dyn OperatorApi>,
    interceptors: Arc<EdgeInterceptors>,
}

impl std::fmt::Debug for OperatorService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OperatorService").finish_non_exhaustive()
    }
}

impl OperatorService {
    pub fn new(api: Arc<dyn OperatorApi>, interceptors: Arc<EdgeInterceptors>) -> Self {
        Self { api, interceptors }
    }

    pub async fn cluster_info(&self, headers: &HeaderMap) -> EdgeResult<ClusterInfo> {
        let _ctx = self
            .interceptors
            .begin(headers, None, Action::OperatorRead, false)
            .await?;

        self.api.cluster_info().await.map_err(EdgeError::from)
    }

    pub async fn list_search_attributes(
        &self,
        headers: &HeaderMap,
        namespace: Option<&str>,
    ) -> EdgeResult<Vec<SearchAttributeDefinition>> {
        let _ctx = self
            .interceptors
            .begin(headers, namespace, Action::OperatorRead, false)
            .await?;

        self.api
            .list_search_attributes(namespace)
            .await
            .map_err(EdgeError::from)
    }

    pub async fn upsert_search_attribute(
        &self,
        headers: &HeaderMap,
        namespace: &str,
        attr: SearchAttributeDefinition,
    ) -> EdgeResult<()> {
        let _ctx = self
            .interceptors
            .begin(headers, Some(namespace), Action::OperatorWrite, false)
            .await?;

        self.api
            .upsert_search_attribute(namespace, attr)
            .await
            .map_err(EdgeError::from)
    }

    pub async fn remove_search_attribute(
        &self,
        headers: &HeaderMap,
        namespace: &str,
        attr_name: &str,
    ) -> EdgeResult<()> {
        let _ctx = self
            .interceptors
            .begin(headers, Some(namespace), Action::OperatorWrite, false)
            .await?;

        self.api
            .remove_search_attribute(namespace, attr_name)
            .await
            .map_err(EdgeError::from)
    }
}
