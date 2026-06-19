use std::{collections::BTreeMap, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use http::HeaderMap;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{
    errors::{EdgeError, EdgeResult},
    interceptors::{Action, EdgeInterceptors},
    nexus_endpoint::NexusEndpointAdmin,
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
    pub shard_count: i32,
    pub supported_clients: BTreeMap<String, String>,
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

    /// Seed a namespace's system/predefined search attributes so visibility
    /// queries in it resolve the map-backed predefined fields. Invoked when a
    /// namespace is registered; must be idempotent. Predefined attributes are
    /// registered only in the visibility store's registry, never the user catalog,
    /// so they never surface as user-defined attributes in `list_search_attributes`
    /// (`common/searchattribute/sadefs/constants.go @ v1.31.0`). The default is a
    /// no-op for implementations without a visibility store (e.g. test/in-memory
    /// catalogs); the store-backed implementation overrides it.
    async fn seed_predefined_search_attributes(&self, _namespace: &str) -> Result<()> {
        Ok(())
    }
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
                shard_count: 1,
                supported_clients: BTreeMap::from([
                    ("temporal-go".to_string(), ">=1.26.0".to_string()),
                    ("temporal-java".to_string(), ">=1.22.0".to_string()),
                    ("temporal-python".to_string(), ">=1.6.0".to_string()),
                    ("temporal-typescript".to_string(), ">=1.10.0".to_string()),
                ]),
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
            None => attrs.values().flat_map(|m| m.values().cloned()).collect(),
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
    /// The Nexus endpoint admin, when standalone endpoint CRUD is wired. `None`
    /// answers `UNIMPLEMENTED` (the pre-feature behaviour); `Some` serves the five
    /// `*NexusEndpoint(s)` RPCs, each gated through the operator interceptor below.
    nexus_endpoints: Option<Arc<NexusEndpointAdmin>>,
}

impl std::fmt::Debug for OperatorService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OperatorService").finish_non_exhaustive()
    }
}

impl OperatorService {
    pub fn new(api: Arc<dyn OperatorApi>, interceptors: Arc<EdgeInterceptors>) -> Self {
        Self {
            api,
            interceptors,
            nexus_endpoints: None,
        }
    }

    /// Attach the Nexus endpoint admin, enabling the `*NexusEndpoint(s)` RPCs.
    pub fn with_nexus_endpoints(mut self, admin: Arc<NexusEndpointAdmin>) -> Self {
        self.nexus_endpoints = Some(admin);
        self
    }

    /// The admin, or the `UNIMPLEMENTED` error when endpoint CRUD is not wired. The
    /// message matches the historical stub so the unconfigured surface is unchanged.
    fn nexus_admin(&self, op: &str) -> EdgeResult<&Arc<NexusEndpointAdmin>> {
        self.nexus_endpoints
            .as_ref()
            .ok_or_else(|| EdgeError::Unimplemented(op.to_owned()))
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

    // === Nexus endpoint admin (global resources; authz-gated like every other
    // operator RPC). Create/Update/Delete require `OperatorWrite`; Get/List require
    // `OperatorRead`. `namespace = None` because endpoints are cluster-global. Each
    // first passes the operator interceptor, then delegates to the admin — closing
    // the gap where the bare gRPC stubs bypassed authorization entirely. ===

    pub async fn create_nexus_endpoint(
        &self,
        headers: &HeaderMap,
        req: tokeira_proto::operatorservice::CreateNexusEndpointRequest,
    ) -> EdgeResult<tokeira_proto::operatorservice::CreateNexusEndpointResponse> {
        let _ctx = self
            .interceptors
            .begin(headers, None, Action::OperatorWrite, false)
            .await?;
        self.nexus_admin("create_nexus_endpoint")?.create(req).await
    }

    pub async fn get_nexus_endpoint(
        &self,
        headers: &HeaderMap,
        req: tokeira_proto::operatorservice::GetNexusEndpointRequest,
    ) -> EdgeResult<tokeira_proto::operatorservice::GetNexusEndpointResponse> {
        let _ctx = self
            .interceptors
            .begin(headers, None, Action::OperatorRead, false)
            .await?;
        self.nexus_admin("get_nexus_endpoint")?.get(req).await
    }

    pub async fn update_nexus_endpoint(
        &self,
        headers: &HeaderMap,
        req: tokeira_proto::operatorservice::UpdateNexusEndpointRequest,
    ) -> EdgeResult<tokeira_proto::operatorservice::UpdateNexusEndpointResponse> {
        let _ctx = self
            .interceptors
            .begin(headers, None, Action::OperatorWrite, false)
            .await?;
        self.nexus_admin("update_nexus_endpoint")?.update(req).await
    }

    pub async fn delete_nexus_endpoint(
        &self,
        headers: &HeaderMap,
        req: tokeira_proto::operatorservice::DeleteNexusEndpointRequest,
    ) -> EdgeResult<tokeira_proto::operatorservice::DeleteNexusEndpointResponse> {
        let _ctx = self
            .interceptors
            .begin(headers, None, Action::OperatorWrite, false)
            .await?;
        self.nexus_admin("delete_nexus_endpoint")?.delete(req).await
    }

    pub async fn list_nexus_endpoints(
        &self,
        headers: &HeaderMap,
        req: tokeira_proto::operatorservice::ListNexusEndpointsRequest,
    ) -> EdgeResult<tokeira_proto::operatorservice::ListNexusEndpointsResponse> {
        let _ctx = self
            .interceptors
            .begin(headers, None, Action::OperatorRead, false)
            .await?;
        self.nexus_admin("list_nexus_endpoints")?.list(req).await
    }
}
