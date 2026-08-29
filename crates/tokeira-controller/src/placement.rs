//! Placement and routing-snapshot computation.

use std::collections::{HashMap, HashSet};

use time::{Duration, OffsetDateTime};
use tokeira_storage::BundleLease;
use tokeira_types::{
    BundleOwner, GenerationCounter, IncarnationId, NodeEndpoint, PlacementConfig, RoutingDelta,
    RoutingSnapshot, ShardEpoch, ShardId,
};
use uuid::Uuid;

use crate::membership::LiveMembership;

/// Runtime placement directive for one membership stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DesiredPlacementDirective {
    pub(crate) acquire_bundles: Vec<ShardId>,
    pub(crate) relinquish_bundles: Vec<ShardId>,
}

/// Per-node DSQL connection budget share.
#[derive(Clone, Debug, PartialEq)]
pub struct ConnectionBudgetDirective {
    pub(crate) rate_per_second: f64,
    pub(crate) capacity: u64,
    pub(crate) max_reservoir_size: u32,
    pub(crate) valid_until: OffsetDateTime,
}

/// Compute actual routing state from DSQL lease rows alone.
pub(crate) fn compute_routing_snapshot(
    leases: &[BundleLease],
    placement_config: PlacementConfig,
    previous: &RoutingSnapshot,
) -> (RoutingSnapshot, RoutingDelta) {
    let mut execution_bundle_owners = HashMap::new();
    let mut node_endpoints = HashMap::new();
    for lease in leases {
        let Some(owner) = lease.owner_node_id.as_deref() else {
            continue;
        };
        let Ok(node_id) = owner.parse::<IncarnationId>() else {
            tracing::warn!(owner, "skipping lease with malformed owner incarnation id");
            continue;
        };
        let Some(endpoint) = lease.node_endpoint.as_deref() else {
            tracing::warn!(
                bundle = lease.bundle_id.0,
                "skipping lease with missing endpoint"
            );
            continue;
        };
        let Ok(endpoint) = endpoint.parse::<NodeEndpoint>() else {
            tracing::warn!(
                bundle = lease.bundle_id.0,
                "skipping lease with malformed endpoint"
            );
            continue;
        };
        execution_bundle_owners.insert(
            lease.bundle_id,
            BundleOwner {
                node_id,
                epoch: lease.epoch,
            },
        );
        node_endpoints.insert(node_id, endpoint);
    }

    let generation = previous.generation.next();
    let snapshot = RoutingSnapshot {
        execution_bundle_owners: execution_bundle_owners.clone(),
        node_endpoints: node_endpoints.clone(),
        placement_config,
        generation,
    };
    let delta = diff_snapshot(previous, &snapshot);
    (snapshot, delta)
}

fn diff_snapshot(previous: &RoutingSnapshot, next: &RoutingSnapshot) -> RoutingDelta {
    let mut bundle_ids = previous
        .execution_bundle_owners
        .keys()
        .copied()
        .collect::<HashSet<_>>();
    bundle_ids.extend(next.execution_bundle_owners.keys().copied());
    let mut bundle_updates = bundle_ids
        .into_iter()
        .filter_map(|bundle_id| {
            let old = previous.execution_bundle_owners.get(&bundle_id);
            let new = next.execution_bundle_owners.get(&bundle_id);
            (old != new).then(|| (bundle_id, new.copied()))
        })
        .collect::<Vec<_>>();
    bundle_updates.sort_by_key(|(bundle_id, _)| bundle_id.0);

    let mut node_ids = previous
        .node_endpoints
        .keys()
        .copied()
        .collect::<HashSet<_>>();
    node_ids.extend(next.node_endpoints.keys().copied());
    let mut node_updates = node_ids
        .into_iter()
        .filter_map(|node_id| {
            let old = previous.node_endpoints.get(&node_id);
            let new = next.node_endpoints.get(&node_id);
            (old != new).then(|| (node_id, new.cloned()))
        })
        .collect::<Vec<_>>();
    node_updates.sort_by_key(|(node_id, _)| node_id.0);

    RoutingDelta {
        base_generation: previous.generation,
        bundle_updates,
        node_updates,
        generation: next.generation,
    }
}

/// Compute simple desired placement for currently unowned bundles.
pub(crate) fn compute_desired_placement(
    membership: &LiveMembership,
    leases: &[BundleLease],
    bundle_count: u32,
) -> HashMap<IncarnationId, DesiredPlacementDirective> {
    let nodes = membership.active_node_ids_sorted();
    if nodes.is_empty() {
        return HashMap::new();
    }
    let owned = leases
        .iter()
        .filter(|lease| lease.owner_node_id.is_some())
        .map(|lease| lease.bundle_id)
        .collect::<HashSet<_>>();
    let mut directives = nodes
        .iter()
        .copied()
        .map(|node_id| {
            (
                node_id,
                DesiredPlacementDirective {
                    acquire_bundles: Vec::new(),
                    relinquish_bundles: Vec::new(),
                },
            )
        })
        .collect::<HashMap<_, _>>();
    for bundle in 0..bundle_count {
        let shard_id = ShardId(bundle);
        if owned.contains(&shard_id) {
            continue;
        }
        let node_id = nodes[usize::try_from(bundle).unwrap_or_default() % nodes.len()];
        if let Some(directive) = directives.get_mut(&node_id) {
            directive.acquire_bundles.push(shard_id);
        }
    }
    directives
}

/// Split cluster connection budget across active nodes deterministically.
pub(crate) fn compute_connection_budget(
    cluster_rate: f64,
    cluster_capacity: u64,
    active_nodes_sorted: &[IncarnationId],
    valid_duration: Duration,
    max_reservoir_size: u32,
) -> Vec<(IncarnationId, ConnectionBudgetDirective)> {
    if active_nodes_sorted.is_empty() {
        return Vec::new();
    }
    let valid_until = OffsetDateTime::now_utc() + valid_duration;
    let node_count = active_nodes_sorted.len() as u64;
    let base_capacity = cluster_capacity / node_count;
    let remainder = cluster_capacity % node_count;
    let per_node_rate = cluster_rate / active_nodes_sorted.len() as f64;
    active_nodes_sorted
        .iter()
        .enumerate()
        .map(|(index, node_id)| {
            let extra = u64::from((index as u64) < remainder);
            (
                *node_id,
                ConnectionBudgetDirective {
                    rate_per_second: per_node_rate,
                    capacity: base_capacity + extra,
                    max_reservoir_size,
                    valid_until,
                },
            )
        })
        .collect()
}

pub(crate) fn empty_previous_snapshot(placement_config: PlacementConfig) -> RoutingSnapshot {
    RoutingSnapshot {
        execution_bundle_owners: HashMap::new(),
        node_endpoints: HashMap::new(),
        placement_config,
        generation: GenerationCounter::ZERO,
    }
}

pub fn lease_for_test(bundle_id: ShardId, node_id: IncarnationId, endpoint: &str) -> BundleLease {
    BundleLease {
        bundle_id,
        owner_node_id: Some(node_id.to_string()),
        epoch: ShardEpoch(1),
        lease_until: OffsetDateTime::now_utc(),
        node_endpoint: Some(endpoint.to_owned()),
    }
}

pub fn allocator_id_for_node(node_id: IncarnationId) -> Uuid {
    node_id.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::membership::{RuntimeHeartbeat, RuntimeRegistration};

    fn placement_config() -> PlacementConfig {
        PlacementConfig {
            shard_count: 4,
            bundle_count: 4,
            partition_count: 16,
            hash_version: 1,
        }
    }

    #[test]
    fn snapshot_uses_parseable_dsql_lease_rows() {
        let node_id = IncarnationId::new();
        let previous = empty_previous_snapshot(placement_config());
        let (snapshot, delta) = compute_routing_snapshot(
            &[lease_for_test(ShardId(1), node_id, "127.0.0.1:7233")],
            placement_config(),
            &previous,
        );
        assert_eq!(
            snapshot
                .lookup_bundle_owner(ShardId(1))
                .map(|owner| owner.node_id),
            Some(node_id)
        );
        assert_eq!(delta.base_generation, GenerationCounter::ZERO);
        assert_eq!(delta.generation, GenerationCounter(1));
    }

    #[test]
    fn malformed_lease_rows_are_skipped() {
        let previous = empty_previous_snapshot(placement_config());
        let (snapshot, _delta) = compute_routing_snapshot(
            &[BundleLease {
                bundle_id: ShardId(1),
                owner_node_id: Some("not-a-uuid".to_owned()),
                epoch: ShardEpoch(1),
                lease_until: OffsetDateTime::now_utc(),
                node_endpoint: Some("127.0.0.1:7233".to_owned()),
            }],
            placement_config(),
            &previous,
        );
        assert!(snapshot.execution_bundle_owners.is_empty());
    }

    #[test]
    fn desired_placement_assigns_unowned_bundles_to_active_nodes() {
        let mut membership = LiveMembership::default();
        let node_id = IncarnationId::new();
        membership.register_node(
            RuntimeRegistration {
                node_id,
                host: "127.0.0.1".to_owned(),
                port: 7233,
                zone: None,
                version: "test".to_owned(),
                build_id: "test".to_owned(),
            },
            RuntimeHeartbeat::empty(),
            None,
        );
        let directives = compute_desired_placement(&membership, &[], 2);
        assert_eq!(
            directives.get(&node_id).map(|d| d.acquire_bundles.clone()),
            Some(vec![ShardId(0), ShardId(1)])
        );
    }

    #[test]
    fn budget_distribution_handles_remainder_deterministically() {
        let mut nodes = vec![IncarnationId::new(), IncarnationId::new()];
        nodes.sort_by_key(|id| id.0);
        let directives = compute_connection_budget(10.0, 11, &nodes, Duration::seconds(30), 5);
        assert_eq!(directives[0].1.capacity, 6);
        assert_eq!(directives[1].1.capacity, 5);
    }
}
