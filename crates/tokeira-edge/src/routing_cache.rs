//! Lock-free routing snapshot cache used by edge request dispatch.

use std::{future::Future, sync::Arc};

use anyhow::{Result, anyhow};
use arc_swap::ArcSwap;
use thiserror::Error;
use tokeira_proto::controller::{
    self as proto, RefreshBundleResponse, RoutingUpdate,
    placement_controller_client::PlacementControllerClient, routing_update::Update,
};
use tokeira_storage::LeaseRepository;
use tokeira_types::{
    BundleId, BundleOwner, GenerationCounter, IncarnationId, NamespaceId, NodeEndpoint,
    PlacementConfig, QueuePartitionKey, RoutingDelta, RoutingSnapshot, ShardEpoch, ShardId,
    WorkflowId, execution_home_bundle,
};
use tokio_stream::StreamExt;

/// Runtime ownership redirect returned by stale-route handling.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NotShardOwner {
    pub bundle_id: BundleId,
    pub current_epoch: ShardEpoch,
    pub current_owner_node_id: Option<IncarnationId>,
}

#[derive(Debug, Error)]
pub enum RoutingError {
    #[error("not shard owner")]
    NotShardOwner(NotShardOwner),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EdgeRoutingConfig {
    pub controller_endpoint: String,
    pub max_retries: usize,
}

/// Edge-local cache of controller-published routing state.
#[derive(Debug)]
pub struct RoutingCache {
    snapshot: ArcSwap<RoutingSnapshot>,
}

impl RoutingCache {
    /// Build a cache with an empty snapshot for the supplied placement config.
    pub fn new(placement_config: PlacementConfig) -> Self {
        Self {
            snapshot: ArcSwap::from_pointee(RoutingSnapshot::new(placement_config)),
        }
    }

    /// Replace the full routing snapshot atomically.
    pub fn replace_snapshot(&self, snapshot: RoutingSnapshot) {
        self.snapshot.store(Arc::new(snapshot));
    }

    /// Return a clone of the current snapshot handle.
    pub fn snapshot(&self) -> Arc<RoutingSnapshot> {
        self.snapshot.load_full()
    }

    /// Resolve the execution-home endpoint and epoch for a workflow identity.
    pub fn resolve_execution_home(
        &self,
        namespace_id: NamespaceId,
        workflow_id: &WorkflowId,
    ) -> Option<(IncarnationId, NodeEndpoint, ShardEpoch)> {
        let snapshot = self.snapshot.load();
        let bundle_id = execution_home_bundle(
            namespace_id.0.as_bytes(),
            workflow_id.0.as_bytes(),
            snapshot.placement_config.bundle_count,
        );
        let owner = snapshot.lookup_bundle_owner(bundle_id)?;
        let endpoint = snapshot.lookup_node_endpoint(owner.node_id)?;
        Some((owner.node_id, endpoint.clone(), owner.epoch))
    }

    pub fn resolve_execution_route(
        &self,
        namespace_id: NamespaceId,
        workflow_id: &WorkflowId,
    ) -> Option<RoutedBundle> {
        let snapshot = self.snapshot.load();
        let bundle_id = execution_home_bundle(
            namespace_id.0.as_bytes(),
            workflow_id.0.as_bytes(),
            snapshot.placement_config.bundle_count,
        );
        let owner = snapshot.lookup_bundle_owner(bundle_id)?;
        let endpoint = snapshot.lookup_node_endpoint(owner.node_id)?;
        Some(RoutedBundle {
            bundle_id,
            node_id: owner.node_id,
            endpoint: endpoint.clone(),
            epoch: owner.epoch,
        })
    }

    /// Resolve queue-home endpoint and epoch from a queue partition key.
    pub fn resolve_queue_home(
        &self,
        key: &QueuePartitionKey,
    ) -> Option<(IncarnationId, NodeEndpoint, ShardEpoch)> {
        let snapshot = self.snapshot.load();
        let owner = snapshot.resolve_queue_home(key)?;
        let endpoint = snapshot.lookup_node_endpoint(owner.node_id)?;
        Some((owner.node_id, endpoint.clone(), owner.epoch))
    }

    pub fn resolve_queue_route(&self, key: &QueuePartitionKey) -> Option<RoutedBundle> {
        let snapshot = self.snapshot.load();
        let bundle_id = tokeira_types::bundle_for_partition(
            key.partition,
            snapshot.placement_config.bundle_count,
        );
        let owner = snapshot.lookup_bundle_owner(bundle_id)?;
        let endpoint = snapshot.lookup_node_endpoint(owner.node_id)?;
        Some(RoutedBundle {
            bundle_id,
            node_id: owner.node_id,
            endpoint: endpoint.clone(),
            epoch: owner.epoch,
        })
    }

    /// Remove one bundle owner from the cached snapshot.
    pub fn invalidate_bundle(&self, bundle_id: BundleId) {
        let mut snapshot = (*self.snapshot.load_full()).clone();
        snapshot.execution_bundle_owners.remove(&bundle_id);
        self.snapshot.store(Arc::new(snapshot));
    }

    /// Apply an owner/endpoint pair learned from DSQL fallback.
    pub fn apply_dsql_fallback(
        &self,
        bundle_id: BundleId,
        owner: BundleOwner,
        endpoint: NodeEndpoint,
    ) {
        let mut snapshot = (*self.snapshot.load_full()).clone();
        snapshot.execution_bundle_owners.insert(bundle_id, owner);
        snapshot.node_endpoints.insert(owner.node_id, endpoint);
        self.snapshot.store(Arc::new(snapshot));
    }

    /// Look up an endpoint by incarnation id.
    pub fn lookup_node_endpoint(&self, node_id: IncarnationId) -> Option<NodeEndpoint> {
        self.snapshot.load().lookup_node_endpoint(node_id).cloned()
    }

    pub fn apply_update(&self, update: RoutingUpdate) -> Result<()> {
        match update.update {
            Some(Update::Full(full)) => {
                self.replace_snapshot(decode_snapshot(full)?);
                Ok(())
            }
            Some(Update::Delta(delta)) => {
                let mut snapshot = (*self.snapshot.load_full()).clone();
                snapshot.apply_delta(decode_delta(delta)?)?;
                self.replace_snapshot(snapshot);
                Ok(())
            }
            None => Ok(()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoutedBundle {
    pub bundle_id: BundleId,
    pub node_id: IncarnationId,
    pub endpoint: NodeEndpoint,
    pub epoch: ShardEpoch,
}

pub async fn subscribe_routing_once(
    cache: Arc<RoutingCache>,
    controller_endpoint: String,
) -> Result<()> {
    let mut client = PlacementControllerClient::connect(controller_endpoint).await?;
    let mut stream = client
        .subscribe_routing(proto::SubscribeRoutingRequest {})
        .await?
        .into_inner();
    if let Some(update) = stream.next().await {
        cache.apply_update(update?)?;
    }
    Ok(())
}

pub async fn run_routing_subscription(
    cache: Arc<RoutingCache>,
    config: EdgeRoutingConfig,
    shutdown: tokio_util::sync::CancellationToken,
) -> Result<()> {
    let mut backoff = std::time::Duration::from_secs(1);
    loop {
        if shutdown.is_cancelled() {
            return Ok(());
        }
        match subscribe_until_disconnect(
            Arc::clone(&cache),
            config.controller_endpoint.clone(),
            shutdown.clone(),
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(error) => {
                tracing::warn!(%error, "routing subscription disconnected; serving stale cache");
            }
        }
        tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            _ = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(std::time::Duration::from_secs(30));
    }
}

async fn subscribe_until_disconnect(
    cache: Arc<RoutingCache>,
    controller_endpoint: String,
    shutdown: tokio_util::sync::CancellationToken,
) -> Result<()> {
    let mut client = PlacementControllerClient::connect(controller_endpoint).await?;
    let mut stream = client
        .subscribe_routing(proto::SubscribeRoutingRequest {})
        .await?
        .into_inner();
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            update = stream.next() => {
                let Some(update) = update else {
                    return Err(anyhow!("routing subscription ended"));
                };
                cache.apply_update(update?)?;
            }
        }
    }
}

pub async fn route_with_retry<T, F, Fut, R>(
    cache: &RoutingCache,
    leases: &R,
    bundle_id: BundleId,
    initial_endpoint: NodeEndpoint,
    initial_epoch: ShardEpoch,
    max_retries: usize,
    make_request: F,
) -> Result<T, RoutingError>
where
    F: FnMut(NodeEndpoint, ShardEpoch) -> Fut,
    Fut: Future<Output = Result<T, RoutingError>>,
    R: LeaseRepository + ?Sized,
{
    route_with_retry_with_refresh(
        cache,
        leases,
        bundle_id,
        initial_endpoint,
        initial_epoch,
        max_retries,
        make_request,
        |_bundle_id| std::future::ready(Ok(None)),
    )
    .await
}

pub async fn route_with_retry_with_refresh<T, F, Fut, R, C, CFut>(
    cache: &RoutingCache,
    leases: &R,
    bundle_id: BundleId,
    initial_endpoint: NodeEndpoint,
    initial_epoch: ShardEpoch,
    max_retries: usize,
    mut make_request: F,
    mut refresh_bundle: C,
) -> Result<T, RoutingError>
where
    F: FnMut(NodeEndpoint, ShardEpoch) -> Fut,
    Fut: Future<Output = Result<T, RoutingError>>,
    R: LeaseRepository + ?Sized,
    C: FnMut(BundleId) -> CFut,
    CFut: Future<Output = Result<Option<(NodeEndpoint, ShardEpoch)>, RoutingError>>,
{
    let mut endpoint = initial_endpoint;
    let mut epoch = initial_epoch;
    for _attempt in 0..=max_retries {
        match make_request(endpoint.clone(), epoch).await {
            Ok(value) => return Ok(value),
            Err(RoutingError::NotShardOwner(not_owner)) => {
                cache.invalidate_bundle(not_owner.bundle_id);
                let mut latest_not_owner = not_owner;
                if let Some(node_id) = latest_not_owner.current_owner_node_id
                    && let Some(hinted_endpoint) = cache.lookup_node_endpoint(node_id)
                {
                    match make_request(hinted_endpoint, latest_not_owner.current_epoch).await {
                        Ok(value) => return Ok(value),
                        Err(RoutingError::NotShardOwner(next_not_owner)) => {
                            latest_not_owner = next_not_owner;
                            cache.invalidate_bundle(latest_not_owner.bundle_id);
                        }
                        Err(err) => return Err(err),
                    }
                }
                if let Some((refreshed_endpoint, refreshed_epoch)) =
                    refresh_bundle(bundle_id).await?
                {
                    endpoint = refreshed_endpoint;
                    epoch = refreshed_epoch;
                    continue;
                }
                if let Some((fallback_endpoint, fallback_epoch)) =
                    apply_dsql_fallback(cache, leases, bundle_id).await?
                {
                    endpoint = fallback_endpoint;
                    epoch = fallback_epoch;
                    continue;
                }
                return Err(RoutingError::NotShardOwner(latest_not_owner));
            }
            Err(err) => return Err(err),
        }
    }
    Err(RoutingError::Other(anyhow!(
        "routing retry budget exhausted"
    )))
}

pub async fn refresh_bundle_from_controller(
    cache: &RoutingCache,
    controller_endpoint: String,
    bundle_id: BundleId,
) -> Result<Option<(NodeEndpoint, ShardEpoch)>, RoutingError> {
    let mut client = PlacementControllerClient::connect(controller_endpoint)
        .await
        .map_err(|err| RoutingError::Other(anyhow!(err)))?;
    let response = client
        .refresh_bundle(tonic::Request::new(proto::RefreshBundleRequest {
            bundle_id: bundle_id.0,
        }))
        .await
        .map_err(|err| RoutingError::Other(anyhow!(err)))?
        .into_inner();
    apply_refresh_response(cache, bundle_id, response)
}

fn apply_refresh_response(
    cache: &RoutingCache,
    bundle_id: BundleId,
    response: RefreshBundleResponse,
) -> Result<Option<(NodeEndpoint, ShardEpoch)>, RoutingError> {
    let Some(bundle) = response.bundle else {
        return Ok(None);
    };
    if ShardId(bundle.bundle_id) != bundle_id {
        return Err(RoutingError::Other(anyhow!(
            "controller refresh returned bundle {} for requested bundle {}",
            bundle.bundle_id,
            bundle_id.0
        )));
    }
    let Some(proto::bundle_ownership_entry::State::Owner(owner)) = bundle.state else {
        return Ok(None);
    };
    let node_id = owner
        .owner_node_id
        .parse::<IncarnationId>()
        .map_err(|err| RoutingError::Other(anyhow!(err)))?;
    let Some(node) = response.node else {
        return Ok(None);
    };
    if node.node_id.parse::<IncarnationId>().ok() != Some(node_id) {
        return Err(RoutingError::Other(anyhow!(
            "controller refresh node entry does not match bundle owner"
        )));
    }
    let Some(proto::node_endpoint_entry::State::Endpoint(endpoint)) = node.state else {
        return Ok(None);
    };
    let endpoint = NodeEndpoint {
        host: endpoint.host,
        port: u16::try_from(endpoint.port).map_err(|err| RoutingError::Other(anyhow!(err)))?,
    };
    let epoch = ShardEpoch(owner.epoch);
    cache.apply_dsql_fallback(bundle_id, BundleOwner { node_id, epoch }, endpoint.clone());
    Ok(Some((endpoint, epoch)))
}

async fn apply_dsql_fallback<R>(
    cache: &RoutingCache,
    leases: &R,
    bundle_id: BundleId,
) -> Result<Option<(NodeEndpoint, ShardEpoch)>, RoutingError>
where
    R: LeaseRepository + ?Sized,
{
    let leases = leases
        .list_bundle_leases()
        .await
        .map_err(|err| RoutingError::Other(anyhow!(err)))?;
    let Some(lease) = leases
        .into_iter()
        .find(|lease| lease.bundle_id == bundle_id)
    else {
        return Ok(None);
    };
    let Some(owner) = lease.owner_node_id.as_deref() else {
        return Ok(None);
    };
    let Some(endpoint) = lease.node_endpoint.as_deref() else {
        return Ok(None);
    };
    let node_id = owner
        .parse::<IncarnationId>()
        .map_err(|err| RoutingError::Other(anyhow!(err)))?;
    let endpoint = endpoint
        .parse::<NodeEndpoint>()
        .map_err(|err| RoutingError::Other(anyhow!(err)))?;
    cache.apply_dsql_fallback(
        bundle_id,
        BundleOwner {
            node_id,
            epoch: lease.epoch,
        },
        endpoint.clone(),
    );
    Ok(Some((endpoint, lease.epoch)))
}

fn decode_snapshot(full: proto::FullRoutingSnapshot) -> Result<RoutingSnapshot> {
    let placement_config = full
        .placement_config
        .ok_or_else(|| anyhow!("routing snapshot missing placement_config"))?;
    let mut snapshot = RoutingSnapshot {
        execution_bundle_owners: Default::default(),
        node_endpoints: Default::default(),
        placement_config: PlacementConfig {
            shard_count: placement_config.shard_count,
            bundle_count: placement_config.bundle_count,
            partition_count: placement_config.partition_count,
            hash_version: placement_config.hash_version,
        },
        generation: GenerationCounter(full.generation),
    };
    for entry in full.bundles {
        if let Some(proto::bundle_ownership_entry::State::Owner(owner)) = entry.state {
            snapshot.execution_bundle_owners.insert(
                ShardId(entry.bundle_id),
                BundleOwner {
                    node_id: owner.owner_node_id.parse()?,
                    epoch: ShardEpoch(owner.epoch),
                },
            );
        }
    }
    for entry in full.nodes {
        if let Some(proto::node_endpoint_entry::State::Endpoint(endpoint)) = entry.state {
            snapshot.node_endpoints.insert(
                entry.node_id.parse()?,
                NodeEndpoint {
                    host: endpoint.host,
                    port: u16::try_from(endpoint.port)?,
                },
            );
        }
    }
    Ok(snapshot)
}

fn decode_delta(delta: proto::RoutingDeltaMessage) -> Result<RoutingDelta> {
    let mut bundle_updates = Vec::new();
    for entry in delta.bundle_updates {
        let update = match entry.state {
            Some(proto::bundle_ownership_entry::State::Owner(owner)) => Some(BundleOwner {
                node_id: owner.owner_node_id.parse()?,
                epoch: ShardEpoch(owner.epoch),
            }),
            _ => None,
        };
        bundle_updates.push((ShardId(entry.bundle_id), update));
    }
    let mut node_updates = Vec::new();
    for entry in delta.node_updates {
        let update = match entry.state {
            Some(proto::node_endpoint_entry::State::Endpoint(endpoint)) => Some(NodeEndpoint {
                host: endpoint.host,
                port: u16::try_from(endpoint.port)?,
            }),
            _ => None,
        };
        node_updates.push((entry.node_id.parse()?, update));
    }
    Ok(RoutingDelta {
        base_generation: GenerationCounter(delta.base_generation),
        bundle_updates,
        node_updates,
        generation: GenerationCounter(delta.generation),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tokeira_storage::{InMemoryStore, LeaseRepository};
    use tokeira_types::{
        QueuePartition, QueuePartitionKey, ShardId, TaskKind, TaskQueueName, bundle_for_partition,
        queue_partition_for,
    };

    use super::*;

    fn placement_config() -> PlacementConfig {
        PlacementConfig {
            shard_count: 4,
            bundle_count: 4,
            partition_count: 16,
            hash_version: 1,
        }
    }

    #[test]
    fn resolves_execution_home_from_snapshot() {
        let namespace_id = NamespaceId(uuid::Uuid::from_u128(7));
        let workflow_id = WorkflowId("workflow".to_owned());
        let bundle_id =
            execution_home_bundle(namespace_id.0.as_bytes(), workflow_id.0.as_bytes(), 4);
        let node_id = IncarnationId::new();
        let endpoint = NodeEndpoint {
            host: "127.0.0.1".to_owned(),
            port: 7233,
        };
        let cache = RoutingCache::new(placement_config());
        cache.replace_snapshot(RoutingSnapshot {
            execution_bundle_owners: HashMap::from([(
                bundle_id,
                BundleOwner {
                    node_id,
                    epoch: ShardEpoch(9),
                },
            )]),
            node_endpoints: HashMap::from([(node_id, endpoint.clone())]),
            placement_config: placement_config(),
            generation: Default::default(),
        });

        assert_eq!(
            cache.resolve_execution_home(namespace_id, &workflow_id),
            Some((node_id, endpoint, ShardEpoch(9)))
        );
    }

    #[test]
    fn resolves_queue_home_by_derived_bundle() {
        let namespace_id = NamespaceId(uuid::Uuid::from_u128(11));
        let partition = queue_partition_for(b"queue", 16);
        let bundle_id = bundle_for_partition(partition, 4);
        let node_id = IncarnationId::new();
        let endpoint = NodeEndpoint {
            host: "127.0.0.1".to_owned(),
            port: 7234,
        };
        let cache = RoutingCache::new(placement_config());
        cache.replace_snapshot(RoutingSnapshot {
            execution_bundle_owners: HashMap::from([(
                bundle_id,
                BundleOwner {
                    node_id,
                    epoch: ShardEpoch(5),
                },
            )]),
            node_endpoints: HashMap::from([(node_id, endpoint.clone())]),
            placement_config: placement_config(),
            generation: Default::default(),
        });
        let key = QueuePartitionKey {
            namespace_id,
            task_queue: TaskQueueName("queue".to_owned()),
            task_kind: TaskKind::Workflow,
            partition,
        };

        assert_eq!(
            cache.resolve_queue_home(&key),
            Some((node_id, endpoint, ShardEpoch(5)))
        );
    }

    #[test]
    fn invalidates_bundle_owner() {
        let cache = RoutingCache::new(placement_config());
        let node_id = IncarnationId::new();
        cache.apply_dsql_fallback(
            ShardId(1),
            BundleOwner {
                node_id,
                epoch: ShardEpoch(1),
            },
            NodeEndpoint {
                host: "127.0.0.1".to_owned(),
                port: 7233,
            },
        );
        cache.invalidate_bundle(ShardId(1));
        assert!(cache.snapshot().lookup_bundle_owner(ShardId(1)).is_none());
    }

    #[test]
    fn queue_partition_newtype_import_is_used() {
        assert_eq!(QueuePartition(1), QueuePartition(1));
    }

    #[test]
    fn apply_delta_with_generation_mismatch_returns_error() {
        let cache = RoutingCache::new(placement_config());
        let result = cache.apply_update(RoutingUpdate {
            update: Some(Update::Delta(proto::RoutingDeltaMessage {
                base_generation: 99,
                bundle_updates: Vec::new(),
                node_updates: Vec::new(),
                generation: 100,
            })),
        });

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn retry_uses_hint_endpoint_before_dsql_fallback() {
        let cache = RoutingCache::new(placement_config());
        let hinted = IncarnationId::new();
        let hinted_endpoint = NodeEndpoint {
            host: "hint".to_owned(),
            port: 1,
        };
        cache.replace_snapshot(RoutingSnapshot {
            execution_bundle_owners: HashMap::new(),
            node_endpoints: HashMap::from([(hinted, hinted_endpoint.clone())]),
            placement_config: placement_config(),
            generation: Default::default(),
        });
        let store = InMemoryStore::default();
        let mut calls = Vec::new();
        let result = route_with_retry(
            &cache,
            &store,
            ShardId(1),
            NodeEndpoint {
                host: "stale".to_owned(),
                port: 1,
            },
            ShardEpoch(1),
            2,
            |endpoint, epoch| {
                calls.push((endpoint.clone(), epoch));
                async move {
                    if endpoint.host == "stale" {
                        Err(RoutingError::NotShardOwner(NotShardOwner {
                            bundle_id: ShardId(1),
                            current_epoch: ShardEpoch(2),
                            current_owner_node_id: Some(hinted),
                        }))
                    } else {
                        Ok(endpoint.host)
                    }
                }
            },
        )
        .await
        .unwrap();

        assert_eq!(result, "hint");
        assert_eq!(calls[1], (hinted_endpoint, ShardEpoch(2)));
    }

    #[tokio::test]
    async fn retry_updates_cache_from_dsql_fallback() {
        let cache = RoutingCache::new(placement_config());
        let store = InMemoryStore::default();
        let node_id = IncarnationId::new();
        let acquired = store
            .try_acquire_bundle(ShardId(1), node_id.to_string(), "127.0.0.1:9000".to_owned())
            .await
            .unwrap();
        let tokeira_storage::LeaseOutcome::Acquired { epoch } = acquired else {
            panic!("expected acquire");
        };
        let mut attempts = 0usize;
        let result = route_with_retry(
            &cache,
            &store,
            ShardId(1),
            NodeEndpoint {
                host: "stale".to_owned(),
                port: 1,
            },
            ShardEpoch(1),
            2,
            |endpoint, _epoch| {
                attempts += 1;
                async move {
                    if endpoint.host == "stale" {
                        Err(RoutingError::NotShardOwner(NotShardOwner {
                            bundle_id: ShardId(1),
                            current_epoch: ShardEpoch(2),
                            current_owner_node_id: None,
                        }))
                    } else {
                        Ok(endpoint)
                    }
                }
            },
        )
        .await
        .unwrap();

        assert_eq!(
            result,
            NodeEndpoint {
                host: "127.0.0.1".to_owned(),
                port: 9000,
            }
        );
        assert_eq!(attempts, 2);
        assert_eq!(
            cache.snapshot().lookup_bundle_owner(ShardId(1)).copied(),
            Some(BundleOwner { node_id, epoch })
        );
    }

    #[tokio::test]
    async fn retry_uses_controller_refresh_before_dsql_fallback() {
        let cache = RoutingCache::new(placement_config());
        let store = InMemoryStore::default();
        let node_id = IncarnationId::new();
        let refreshed_endpoint = NodeEndpoint {
            host: "refreshed".to_owned(),
            port: 7233,
        };
        let mut calls = Vec::new();
        let mut refresh_calls = 0usize;

        let result = route_with_retry_with_refresh(
            &cache,
            &store,
            ShardId(2),
            NodeEndpoint {
                host: "stale".to_owned(),
                port: 1,
            },
            ShardEpoch(1),
            2,
            |endpoint, epoch| {
                calls.push((endpoint.clone(), epoch));
                async move {
                    if endpoint.host == "stale" {
                        Err(RoutingError::NotShardOwner(NotShardOwner {
                            bundle_id: ShardId(2),
                            current_epoch: ShardEpoch(8),
                            current_owner_node_id: None,
                        }))
                    } else {
                        Ok(endpoint.host)
                    }
                }
            },
            |bundle_id| {
                refresh_calls += 1;
                cache.apply_dsql_fallback(
                    bundle_id,
                    BundleOwner {
                        node_id,
                        epoch: ShardEpoch(8),
                    },
                    refreshed_endpoint.clone(),
                );
                let endpoint = refreshed_endpoint.clone();
                async move {
                    assert_eq!(bundle_id, ShardId(2));
                    Ok(Some((endpoint, ShardEpoch(8))))
                }
            },
        )
        .await
        .unwrap();

        assert_eq!(result, "refreshed");
        assert_eq!(refresh_calls, 1);
        assert_eq!(
            calls,
            vec![
                (
                    NodeEndpoint {
                        host: "stale".to_owned(),
                        port: 1,
                    },
                    ShardEpoch(1),
                ),
                (refreshed_endpoint, ShardEpoch(8)),
            ]
        );
        assert_eq!(
            cache.snapshot().lookup_bundle_owner(ShardId(2)).copied(),
            Some(BundleOwner {
                node_id,
                epoch: ShardEpoch(8),
            })
        );
    }
}
