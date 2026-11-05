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
    /// Whether every execution route is local. This avoids an extra admission
    /// and state read when a task reply cannot require transport forwarding.
    fn routes_locally(&self) -> bool {
        false
    }

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
    fn routes_locally(&self) -> bool {
        true
    }

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
        // Workflow, activity and Nexus tasks share a queue home: use the same
        // workflow-key partition as PlacementQueueHomes::local_home for every kind.
        let partition = tokeira_types::workflow_queue_partition(
            crate::translate::to_internal::namespace_id_for(namespace),
            task_queue,
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

    use proptest::prelude::*;
    use tokeira_types::{
        BundleOwner, GenerationCounter, PlacementConfig, RoutingSnapshot, ShardEpoch, ShardId,
        bundle_for_partition, execution_home_bundle, queue_partition_for, workflow_queue_partition,
    };
    use uuid::Uuid;

    use super::*;

    // Nexus polls pass TaskKind::Workflow in poll_nexus_task_queue, so these two
    // kinds cover every poll.
    const POLL_TASK_KINDS: [TaskKind; 2] = [TaskKind::Workflow, TaskKind::Activity];

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
    async fn cache_router_routes_all_task_kinds_to_workflow_queue_home() {
        let config = placement_config();
        let local = incarnation(1);
        let remote = incarnation(2);
        let namespace = "default";
        let task_queue = "worker";
        let placement_key = format!(
            "{namespace}\0{task_queue}\0{}",
            TaskKind::Activity.to_db_smallint()
        );
        let old_activity_partition =
            queue_partition_for(placement_key.as_bytes(), config.partition_count);
        let old_activity_bundle = bundle_for_partition(old_activity_partition, config.bundle_count);
        let partition = workflow_queue_partition(
            crate::translate::to_internal::namespace_id_for(namespace),
            task_queue,
            config.partition_count,
        );
        let bundle_id = bundle_for_partition(partition, config.bundle_count);
        assert_ne!(old_activity_bundle, bundle_id);
        let mut snapshot = RoutingSnapshot {
            execution_bundle_owners: HashMap::new(),
            node_endpoints: HashMap::new(),
            placement_config: config,
            generation: GenerationCounter(1),
        };
        snapshot.execution_bundle_owners.insert(
            old_activity_bundle,
            BundleOwner {
                node_id: local,
                epoch: ShardEpoch(11),
            },
        );
        snapshot.execution_bundle_owners.insert(
            bundle_id,
            BundleOwner {
                node_id: remote,
                epoch: ShardEpoch(12),
            },
        );
        assert_ne!(
            snapshot.execution_bundle_owners[&old_activity_bundle].node_id,
            snapshot.execution_bundle_owners[&bundle_id].node_id,
        );
        snapshot.node_endpoints.insert(local, endpoint(8233));
        snapshot.node_endpoints.insert(remote, endpoint(8234));

        let cache = Arc::new(RoutingCache::new(config));
        cache.replace_snapshot(snapshot);
        let router = CacheBackedRouter::new(cache, local);

        for kind in POLL_TASK_KINDS {
            assert_eq!(
                router
                    .route_task_queue(namespace, task_queue, kind)
                    .await
                    .unwrap(),
                RouteTarget::Remote {
                    target: "127.0.0.1:8234".to_owned()
                },
                "{kind:?} must reach the workflow-key queue home",
            );
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        // Every task kind reaches the workflow-key bundle owner from every node.
        #[test]
        fn cache_router_task_kinds_match_workflow_queue_home(
            namespace in "[a-zA-Z0-9_-]{1,40}",
            task_queue in "[a-zA-Z0-9_/-]{1,80}",
            partition_count in 1u32..257,
            owners in prop::collection::vec(0u16..4, 1..33),
        ) {
            let config = PlacementConfig {
                shard_count: owners.len() as u32,
                bundle_count: owners.len() as u32,
                partition_count,
                hash_version: 1,
            };
            let mut snapshot = RoutingSnapshot::new(config);
            for node in 0u16..4 {
                snapshot.node_endpoints.insert(incarnation(u128::from(node)), endpoint(8234 + node));
            }
            for (bundle, node) in owners.iter().enumerate() {
                snapshot.execution_bundle_owners.insert(
                    ShardId(bundle as u32),
                    BundleOwner { node_id: incarnation(u128::from(*node)), epoch: ShardEpoch(1) },
                );
            }
            let home = bundle_for_partition(
                workflow_queue_partition(
                    crate::translate::to_internal::namespace_id_for(&namespace),
                    &task_queue,
                    partition_count,
                ),
                config.bundle_count,
            );
            let expected_owner = owners[home.0 as usize];
            let cache = Arc::new(RoutingCache::new(config));
            cache.replace_snapshot(snapshot);
            let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();

            for local in 0u16..4 {
                let router = CacheBackedRouter::new(cache.clone(), incarnation(u128::from(local)));
                let expected = if local == expected_owner {
                    RouteTarget::Local
                } else {
                    RouteTarget::Remote { target: endpoint(8234 + expected_owner).as_authority() }
                };
                for kind in POLL_TASK_KINDS {
                    let actual = runtime.block_on(router.route_task_queue(&namespace, &task_queue, kind)).unwrap();
                    prop_assert_eq!(&actual, &expected, "kind={:?}, local={}", kind, local);
                }
            }
        }
    }
}
