//! Shard-ownership-aware request forwarding — a transport decision, not a
//! correctness one.
//!
//! In a multi-node deployment a request can land on a node that does not own the
//! target shard. [`EdgeRouter`] resolves whether the request is [`RouteTarget::Local`]
//! or must be forwarded to the owning node ([`RouteTarget::Remote`]), using the
//! controller-published placement snapshot (via [`CacheBackedRouter`] over
//! [`RoutingCache`]). The critical invariant:
//! "remote" means *forward the identical request elsewhere*, never *change how the
//! workflow behaves* — shard ownership is authoritative below the edge, so the edge
//! only chooses where to send the bytes. [`LocalOnlyRouter`] is the single-node
//! default.

use async_trait::async_trait;
use tokeira_types::{
    IncarnationId, NodeEndpoint, QueuePartitionKey, TaskKind, TaskQueueName, WorkflowId,
    queue_partition_for,
};

use crate::{
    errors::{EdgeError, EdgeResult},
    routing_cache::RoutingCache,
};

/// Transport-level routing decision made at the edge.
///
/// This is intentionally *not* a correctness decision. If a request is remote,
/// it means "forward the same request to another edge/runtime node", not "change
/// how the workflow behaves".
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RouteTarget {
    Local,
    Remote { target: String },
}

#[async_trait]
pub trait EdgeRouter: Send + Sync + 'static {
    async fn route_workflow(&self, namespace: &str, workflow_id: &str) -> EdgeResult<RouteTarget>;

    async fn route_task_queue(
        &self,
        namespace: &str,
        task_queue: &str,
        task_kind: TaskKind,
    ) -> EdgeResult<RouteTarget>;
}

#[derive(Debug, Default)]
pub struct LocalOnlyRouter;

#[async_trait]
impl EdgeRouter for LocalOnlyRouter {
    async fn route_workflow(
        &self,
        _namespace: &str,
        _workflow_id: &str,
    ) -> EdgeResult<RouteTarget> {
        Ok(RouteTarget::Local)
    }

    async fn route_task_queue(
        &self,
        _namespace: &str,
        _task_queue: &str,
        _task_kind: TaskKind,
    ) -> EdgeResult<RouteTarget> {
        Ok(RouteTarget::Local)
    }
}

/// Edge router backed by the controller-published placement snapshot.
#[derive(Debug)]
pub struct CacheBackedRouter {
    cache: std::sync::Arc<RoutingCache>,
    local_node_id: IncarnationId,
}

impl CacheBackedRouter {
    pub fn new(cache: std::sync::Arc<RoutingCache>, local_node_id: IncarnationId) -> Self {
        Self {
            cache,
            local_node_id,
        }
    }

    fn target_for(&self, node_id: IncarnationId, endpoint: NodeEndpoint) -> RouteTarget {
        if node_id == self.local_node_id {
            RouteTarget::Local
        } else {
            RouteTarget::Remote {
                target: endpoint.as_authority(),
            }
        }
    }

    fn fallback_node(&self) -> Option<(IncarnationId, NodeEndpoint)> {
        self.cache
            .snapshot()
            .node_endpoints
            .iter()
            .min_by_key(|(node_id, _)| node_id.0)
            .map(|(node_id, endpoint)| (*node_id, endpoint.clone()))
    }
}

#[async_trait]
impl EdgeRouter for CacheBackedRouter {
    async fn route_workflow(&self, namespace: &str, workflow_id: &str) -> EdgeResult<RouteTarget> {
        let namespace_id = crate::translate::to_internal::namespace_id_for(namespace);
        let workflow_id = WorkflowId(workflow_id.to_owned());
        let Some(route) = self
            .cache
            .resolve_execution_route(namespace_id, &workflow_id)
        else {
            return Ok(RouteTarget::Local);
        };
        Ok(self.target_for(route.node_id, route.endpoint))
    }

    async fn route_task_queue(
        &self,
        namespace: &str,
        task_queue: &str,
        task_kind: TaskKind,
    ) -> EdgeResult<RouteTarget> {
        let snapshot = self.cache.snapshot();
        let placement_key = format!("{namespace}\0{task_queue}\0{}", task_kind.to_db_smallint());
        let partition = queue_partition_for(
            placement_key.as_bytes(),
            snapshot.placement_config.partition_count,
        );
        drop(snapshot);

        let key = QueuePartitionKey {
            namespace_id: crate::translate::to_internal::namespace_id_for(namespace),
            task_queue: TaskQueueName(task_queue.to_owned()),
            task_kind,
            partition,
        };
        if let Some(route) = self.cache.resolve_queue_route(&key) {
            return Ok(self.target_for(route.node_id, route.endpoint));
        }
        if let Some((node_id, endpoint)) = self.fallback_node() {
            return Ok(self.target_for(node_id, endpoint));
        }
        Ok(RouteTarget::Local)
    }
}

pub fn ensure_local(route: RouteTarget) -> EdgeResult<()> {
    match route {
        RouteTarget::Local => Ok(()),
        RouteTarget::Remote { target } => Err(EdgeError::RemoteRouteUnsupported { target }),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use tokeira_types::{
        BundleOwner, GenerationCounter, PlacementConfig, RoutingSnapshot, ShardEpoch,
        bundle_for_partition, execution_home_bundle,
    };
    use uuid::Uuid;

    use super::*;

    fn placement_config() -> PlacementConfig {
        PlacementConfig {
            shard_count: 8,
            bundle_count: 8,
            partition_count: 16,
            hash_version: 1,
        }
    }

    fn incarnation(value: u128) -> IncarnationId {
        IncarnationId(Uuid::from_u128(value))
    }

    fn endpoint(port: u16) -> NodeEndpoint {
        NodeEndpoint {
            host: "127.0.0.1".to_owned(),
            port,
        }
    }

    #[tokio::test]
    async fn cache_router_routes_workflow_to_bundle_owner() {
        let config = placement_config();
        let local = incarnation(1);
        let remote = incarnation(2);
        let namespace = "default";
        let workflow_id = "wf";
        let namespace_id = crate::translate::to_internal::namespace_id_for(namespace);
        let bundle_id = execution_home_bundle(namespace_id.0.as_bytes(), workflow_id.as_bytes(), 8);
        let mut snapshot = RoutingSnapshot {
            execution_bundle_owners: HashMap::new(),
            node_endpoints: HashMap::new(),
            placement_config: config,
            generation: GenerationCounter(1),
        };
        snapshot.execution_bundle_owners.insert(
            bundle_id,
            BundleOwner {
                node_id: remote,
                epoch: ShardEpoch(9),
            },
        );
        snapshot.node_endpoints.insert(remote, endpoint(7234));

        let cache = Arc::new(RoutingCache::new(config));
        cache.replace_snapshot(snapshot);
        let router = CacheBackedRouter::new(cache, local);

        assert_eq!(
            router.route_workflow(namespace, workflow_id).await.unwrap(),
            RouteTarget::Remote {
                target: "127.0.0.1:7234".to_owned()
            }
        );
    }

    #[tokio::test]
    async fn cache_router_routes_task_kind_specific_queue_home() {
        let config = placement_config();
        let local = incarnation(1);
        let remote = incarnation(2);
        let namespace = "default";
        let task_queue = "worker";
        let placement_key = format!(
            "{namespace}\0{task_queue}\0{}",
            TaskKind::Activity.to_db_smallint()
        );
        let partition = queue_partition_for(placement_key.as_bytes(), config.partition_count);
        let bundle_id = bundle_for_partition(partition, config.bundle_count);
        let mut snapshot = RoutingSnapshot {
            execution_bundle_owners: HashMap::new(),
            node_endpoints: HashMap::new(),
            placement_config: config,
            generation: GenerationCounter(1),
        };
        snapshot.execution_bundle_owners.insert(
            bundle_id,
            BundleOwner {
                node_id: remote,
                epoch: ShardEpoch(12),
            },
        );
        snapshot.node_endpoints.insert(remote, endpoint(8234));

        let cache = Arc::new(RoutingCache::new(config));
        cache.replace_snapshot(snapshot);
        let router = CacheBackedRouter::new(cache, local);

        assert_eq!(
            router
                .route_task_queue(namespace, task_queue, TaskKind::Activity)
                .await
                .unwrap(),
            RouteTarget::Remote {
                target: "127.0.0.1:8234".to_owned()
            }
        );
    }
}
