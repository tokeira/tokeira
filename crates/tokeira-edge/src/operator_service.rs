use std::{collections::BTreeMap, sync::Arc, time::Duration};

use anyhow::Result;
use async_trait::async_trait;
use http::HeaderMap;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{
    errors::{EdgeError, EdgeResult},
    interceptors::{Action, EdgeInterceptors},
    namespace_cache::{NamespaceCache, ResolvedNamespace},
    nexus_endpoint::NexusEndpointAdmin,
    translate::to_internal::namespace_id_for,
};

/// The v1.31.0 default post-reclaim namespace-delete delay.
///
/// Temporal reads this from `frontend.deleteNamespaceNamespaceDeleteDelay`, whose
/// release default is zero (`common/dynamicconfig/constants.go @ v1.31.0`). Tokeira
/// pins behavioural defaults rather than exposing Temporal's dynamic-config surface.
pub const DEFAULT_NAMESPACE_DELETE_DELAY: Duration = Duration::ZERO;

const NAMESPACE_TOMBSTONE_OBSERVATION_GRACE: Duration = Duration::from_millis(100);

/// Edge representation of OperatorService `DeleteNamespaceRequest`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteNamespaceRequest {
    /// Non-empty name selector, mutually exclusive with [`Self::namespace_id`].
    pub namespace: Option<String>,
    /// Non-empty stable-ID selector, mutually exclusive with [`Self::namespace`].
    pub namespace_id: Option<String>,
    /// Explicit post-reclaim delay. `Some(Duration::ZERO)` is distinct from absence.
    pub namespace_delete_delay: Option<Duration>,
}

/// Edge representation of OperatorService `DeleteNamespaceResponse`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteNamespaceResponse {
    /// Temporary deleted namespace name published while reclaim runs.
    pub deleted_namespace: String,
}

/// Authoritative execution-reclaim capability used by namespace deletion.
///
/// Implementations delete every run for one namespace through the ordinary fenced
/// run-deletion path. Namespace metadata remains an edge/operator concern and is not
/// exposed to this trait.
#[async_trait]
pub trait NamespaceDeletionApi: Send + Sync + 'static {
    /// Delete every authoritative run owned by `namespace_id`.
    async fn reclaim_namespace_runs(&self, namespace_id: tokeira_types::NamespaceId) -> Result<()>;
}

#[derive(Clone)]
struct NamespaceDeletionConfig {
    namespaces: Arc<dyn NamespaceCache>,
    deletion: Arc<dyn NamespaceDeletionApi>,
}

enum NamespaceSelector<'a> {
    Name(&'a str),
    Id(&'a str),
}

fn namespace_selector(req: &DeleteNamespaceRequest) -> EdgeResult<NamespaceSelector<'_>> {
    match (&req.namespace, &req.namespace_id) {
        (None, None) => Err(EdgeError::BadRequest(
            "namespace or namespace ID is required".to_owned(),
        )),
        (Some(_), Some(_)) => Err(EdgeError::BadRequest(
            "Only one of namespace name or Id should be set on request.".to_owned(),
        )),
        (Some(name), None) => Ok(NamespaceSelector::Name(name)),
        (None, Some(namespace_id)) => Ok(NamespaceSelector::Id(namespace_id)),
    }
}

async fn deleted_namespace_name(
    namespaces: &dyn NamespaceCache,
    namespace: &ResolvedNamespace,
) -> EdgeResult<String> {
    let namespace_id = namespace.namespace_id.as_deref().ok_or_else(|| {
        EdgeError::Internal("namespace ID missing during delete-name generation".to_owned())
    })?;
    for suffix_len in 5..=namespace_id.len() {
        let candidate = format!("{}-deleted-{}", namespace.name, &namespace_id[..suffix_len]);
        if namespaces
            .get(&candidate)
            .await
            .map_err(EdgeError::from)?
            .is_none()
        {
            return Ok(candidate);
        }
    }
    Err(EdgeError::Internal(format!(
        "unable to generate a unique deleted name for namespace {}",
        namespace.name
    )))
}

fn namespace_delete_delay(explicit: Option<Duration>) -> Duration {
    explicit.unwrap_or(DEFAULT_NAMESPACE_DELETE_DELAY)
}

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
    /// Builds the in-memory service with the caller's opaque server identity.
    ///
    /// Keeping composition above the compatibility edge prevents this crate from
    /// acquiring build-environment policy while ensuring every discovery response
    /// reports one consistent value.
    pub fn new(cluster_name: impl Into<String>, server_version: impl Into<String>) -> Self {
        Self {
            cluster_info: RwLock::new(ClusterInfo {
                cluster_name: cluster_name.into(),
                version: server_version.into(),
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
    namespace_deletion: Option<NamespaceDeletionConfig>,
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
            namespace_deletion: None,
        }
    }

    /// Attach the Nexus endpoint admin, enabling the `*NexusEndpoint(s)` RPCs.
    pub fn with_nexus_endpoints(mut self, admin: Arc<NexusEndpointAdmin>) -> Self {
        self.nexus_endpoints = Some(admin);
        self
    }

    /// Attach the namespace registry and authoritative reclaim implementation.
    pub fn with_namespace_deletion(
        mut self,
        namespaces: Arc<dyn NamespaceCache>,
        deletion: Arc<dyn NamespaceDeletionApi>,
    ) -> Self {
        self.namespace_deletion = Some(NamespaceDeletionConfig {
            namespaces,
            deletion,
        });
        self
    }

    /// The admin, or the `UNIMPLEMENTED` error when endpoint CRUD is not wired. The
    /// message matches the historical stub so the unconfigured surface is unchanged.
    fn nexus_admin(&self, op: &str) -> EdgeResult<&Arc<NexusEndpointAdmin>> {
        self.nexus_endpoints
            .as_ref()
            .ok_or_else(|| EdgeError::Unimplemented(op.to_owned()))
    }

    fn namespace_deletion(&self) -> EdgeResult<&NamespaceDeletionConfig> {
        self.namespace_deletion
            .as_ref()
            .ok_or_else(|| EdgeError::Unimplemented("delete_namespace".to_owned()))
    }

    /// Mark, rename, and asynchronously reclaim a namespace.
    ///
    /// The response waits for mark-and-rename but not execution reclaim, matching
    /// `DeleteNamespaceWorkflow` starting `ReclaimResourcesWorkflow` and returning
    /// after the child starts (`service/worker/deletenamespace/workflow.go @ v1.31.0`).
    pub async fn delete_namespace(
        &self,
        headers: &HeaderMap,
        req: DeleteNamespaceRequest,
    ) -> EdgeResult<DeleteNamespaceResponse> {
        let _ctx = self
            .interceptors
            .begin(
                headers,
                req.namespace.as_deref(),
                Action::DeleteNamespace,
                false,
            )
            .await?;
        let config = self.namespace_deletion()?.clone();

        let mut namespace = match namespace_selector(&req)? {
            NamespaceSelector::Name(name) => config
                .namespaces
                .get(name)
                .await
                .map_err(EdgeError::from)?
                .ok_or_else(|| EdgeError::NamespaceNotFound(name.to_owned()))?,
            NamespaceSelector::Id(namespace_id) => config
                .namespaces
                .get_by_id(namespace_id)
                .await
                .map_err(EdgeError::from)?
                .ok_or_else(|| EdgeError::NamespaceNotFound(namespace_id.to_owned()))?,
        };

        if namespace.name == "temporal-system" {
            return Err(EdgeError::FailedPrecondition(
                "unable to delete system namespace".to_owned(),
            ));
        }

        let namespace_id = namespace
            .namespace_id
            .as_deref()
            .and_then(|value| uuid::Uuid::parse_str(value).ok())
            .map(tokeira_types::NamespaceId)
            .unwrap_or_else(|| namespace_id_for(&namespace.name));
        namespace.namespace_id = Some(namespace_id.0.to_string());

        let original_name = namespace.name.clone();
        let deleted_name = if namespace.deleted {
            namespace.name.clone()
        } else {
            deleted_namespace_name(config.namespaces.as_ref(), &namespace).await?
        };
        namespace.name.clone_from(&deleted_name);
        namespace.deleted = true;
        if original_name != deleted_name {
            config
                .namespaces
                .replace_name(&original_name, namespace)
                .await
                .map_err(EdgeError::from)?;
        }

        let delay = namespace_delete_delay(req.namespace_delete_delay);
        let response_name = deleted_name.clone();
        tokio::spawn(async move {
            if let Err(error) = config.deletion.reclaim_namespace_runs(namespace_id).await {
                // Retaining the tombstone makes incomplete reclaim observable and lets an
                // operator retry. Removing it after a partial purge would falsely report
                // completion and permit same-name recreation over surviving runs.
                tracing::error!(
                    namespace_id = %namespace_id.0,
                    deleted_namespace = %deleted_name,
                    ?error,
                    "namespace execution reclaim failed; retaining tombstone"
                );
                return;
            }
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            } else {
                // Temporal waits for namespace-cache refresh before reclaim even when the
                // configured delete delay is zero (`namespaceCacheRefreshDelay`,
                // `reclaimresources/workflow.go @ v1.31.0`). Keep a short local grace so
                // the caller can observe the required DELETED tombstone without turning
                // the zero policy default into an operator-visible long delay.
                tokio::time::sleep(NAMESPACE_TOMBSTONE_OBSERVATION_GRACE).await;
            }
            if let Err(error) = config.namespaces.remove(&deleted_name).await {
                tracing::error!(
                    namespace_id = %namespace_id.0,
                    deleted_namespace = %deleted_name,
                    ?error,
                    "namespace tombstone removal failed"
                );
            }
        });

        Ok(DeleteNamespaceResponse {
            deleted_namespace: response_name,
        })
    }

    pub async fn cluster_info(&self, headers: &HeaderMap) -> EdgeResult<ClusterInfo> {
        let _ctx = self
            .interceptors
            .begin(headers, None, Action::GetClusterInfo, false)
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
            .begin(headers, namespace, Action::ListSearchAttributes, false)
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
            .begin(headers, Some(namespace), Action::AddSearchAttributes, false)
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
            .begin(
                headers,
                Some(namespace),
                Action::RemoveSearchAttributes,
                false,
            )
            .await?;

        self.api
            .remove_search_attribute(namespace, attr_name)
            .await
            .map_err(EdgeError::from)
    }

    // === Nexus endpoint admin (global resources; authz-gated like every other
    // operator RPC). All five require cluster Admin in v1.31.0, including Get/List.
    // `namespace = None` because endpoints are cluster-global. Each
    // first passes the operator interceptor, then delegates to the admin — closing
    // the gap where the bare gRPC stubs bypassed authorization entirely. ===

    pub async fn create_nexus_endpoint(
        &self,
        headers: &HeaderMap,
        req: tokeira_proto::operatorservice::CreateNexusEndpointRequest,
    ) -> EdgeResult<tokeira_proto::operatorservice::CreateNexusEndpointResponse> {
        let _ctx = self
            .interceptors
            .begin(headers, None, Action::CreateNexusEndpoint, false)
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
            .begin(headers, None, Action::GetNexusEndpoint, false)
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
            .begin(headers, None, Action::UpdateNexusEndpoint, false)
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
            .begin(headers, None, Action::DeleteNexusEndpoint, false)
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
            .begin(headers, None, Action::ListNexusEndpoints, false)
            .await?;
        self.nexus_admin("list_nexus_endpoints")?.list(req).await
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::namespace_cache::{InMemoryNamespaceCache, NamespaceCache};

    #[tokio::test]
    async fn cluster_info_preserves_the_caller_supplied_server_version() {
        let api = InMemoryOperatorApi::new("tokeira-local", "0.1.0+abcdef12");

        assert_eq!(
            api.cluster_info().await.expect("cluster info").version,
            "0.1.0+abcdef12"
        );
    }

    proptest! {
        /// Exactly one selector is accepted; invalid selector pairs are rejected by a
        /// pure gate before the registry can be mutated.
        // Feature: api-conformance-namespace-full, Property 1: selector validation is mutation-free
        #[test]
        fn namespace_selector_accepts_exactly_one_field(
            namespace in proptest::option::of("[a-z][a-z0-9-]{0,12}"),
            namespace_id in proptest::option::of("[0-9a-f-]{1,36}"),
        ) {
            let request = DeleteNamespaceRequest {
                namespace,
                namespace_id,
                namespace_delete_delay: None,
            };
            prop_assert_eq!(
                namespace_selector(&request).is_ok(),
                request.namespace.is_some() ^ request.namespace_id.is_some()
            );
        }

        /// Explicit request presence always wins over the v1.31.0 zero default.
        // Feature: api-conformance-namespace-full, Property 4: delete-delay precedence controls final removal
        #[test]
        fn explicit_namespace_delete_delay_has_precedence(millis in 0_u64..1_000_000) {
            let explicit = Duration::from_millis(millis);
            prop_assert_eq!(namespace_delete_delay(Some(explicit)), explicit);
            prop_assert_eq!(namespace_delete_delay(None), DEFAULT_NAMESPACE_DELETE_DELAY);
        }

        /// Deleted-name generation extends the stable-ID prefix only across occupied names.
        // Feature: api-conformance-namespace-full, Property 2: mark-and-rename preserves identity
        #[test]
        fn deleted_name_uses_shortest_collision_free_id_prefix(
            original in "[a-z][a-z0-9-]{0,20}",
            collision_count in 0_usize..8,
        ) {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime");
            runtime.block_on(async {
                let namespaces = InMemoryNamespaceCache::new();
                let namespace_id = "12345678-1234-1234-1234-123456789abc";
                for extra in 0..collision_count {
                    let prefix_len = 5 + extra;
                    namespaces
                        .insert(ResolvedNamespace::active(format!(
                            "{original}-deleted-{}",
                            &namespace_id[..prefix_len]
                        )))
                        .await
                        .expect("seed collision");
                }
                let namespace = ResolvedNamespace {
                    namespace_id: Some(namespace_id.to_owned()),
                    ..ResolvedNamespace::active(original.clone())
                };
                let actual = deleted_namespace_name(&namespaces, &namespace)
                    .await
                    .expect("generate deleted name");
                let expected_prefix_len = 5 + collision_count;
                prop_assert_eq!(
                    actual,
                    format!(
                        "{original}-deleted-{}",
                        &namespace_id[..expected_prefix_len]
                    )
                );
                Ok(())
            })?;
        }
    }
}
