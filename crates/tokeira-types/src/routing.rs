//! Placement routing snapshots shared by controller, edge, and runtime.

use std::{collections::HashMap, str::FromStr};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    BundleId, GenerationCounter, IncarnationId, PlacementConfig, QueuePartition, QueuePartitionKey,
    ShardEpoch, ShardId,
};

/// Network endpoint for a runtime incarnation.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeEndpoint {
    pub host: String,
    pub port: u16,
}

impl NodeEndpoint {
    /// Format as the canonical `host:port` lease endpoint string.
    pub fn as_authority(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

impl FromStr for NodeEndpoint {
    type Err = NodeEndpointParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (host, port) = value
            .rsplit_once(':')
            .ok_or(NodeEndpointParseError::MissingPort)?;
        if host.is_empty() {
            return Err(NodeEndpointParseError::MissingHost);
        }
        let port = port
            .parse::<u16>()
            .map_err(|_| NodeEndpointParseError::InvalidPort)?;
        Ok(Self {
            host: host.to_owned(),
            port,
        })
    }
}

/// Endpoint parse failure for DSQL-sourced lease endpoint strings.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum NodeEndpointParseError {
    #[error("node endpoint is missing host")]
    MissingHost,
    #[error("node endpoint is missing port")]
    MissingPort,
    #[error("node endpoint has invalid port")]
    InvalidPort,
}

/// Confirmed owner for a bundle in a routing snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BundleOwner {
    pub node_id: IncarnationId,
    pub epoch: ShardEpoch,
}

/// Convenience value for callers that want the derived queue-home bundle.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QueuePartitionHome {
    pub node_id: IncarnationId,
    pub bundle_id: BundleId,
}

/// Controller-published routing view.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutingSnapshot {
    pub execution_bundle_owners: HashMap<BundleId, BundleOwner>,
    pub node_endpoints: HashMap<IncarnationId, NodeEndpoint>,
    pub placement_config: PlacementConfig,
    pub generation: GenerationCounter,
}

impl RoutingSnapshot {
    /// Create an empty snapshot for a placement configuration.
    pub fn new(placement_config: PlacementConfig) -> Self {
        Self {
            execution_bundle_owners: HashMap::new(),
            node_endpoints: HashMap::new(),
            placement_config,
            generation: GenerationCounter::ZERO,
        }
    }

    /// Look up confirmed execution bundle ownership.
    pub fn lookup_bundle_owner(&self, bundle_id: BundleId) -> Option<&BundleOwner> {
        self.execution_bundle_owners.get(&bundle_id)
    }

    /// Look up the endpoint for an incarnation.
    pub fn lookup_node_endpoint(&self, node_id: IncarnationId) -> Option<&NodeEndpoint> {
        self.node_endpoints.get(&node_id)
    }

    /// Resolve queue-home from the bundle that owns the queue partition.
    pub fn resolve_queue_home(&self, key: &QueuePartitionKey) -> Option<&BundleOwner> {
        let bundle_id = bundle_for_partition(key.partition, self.placement_config.bundle_count);
        self.execution_bundle_owners.get(&bundle_id)
    }

    /// Iterate over all bundle ownership entries.
    pub fn bundle_owners(&self) -> impl Iterator<Item = (BundleId, &BundleOwner)> {
        self.execution_bundle_owners.iter().map(|(&id, owner)| (id, owner))
    }

    /// Iterate over all node endpoint entries.
    pub fn node_endpoints_iter(&self) -> impl Iterator<Item = (IncarnationId, &NodeEndpoint)> {
        self.node_endpoints.iter().map(|(&id, ep)| (id, ep))
    }

    /// Apply a delta if it is based on this snapshot's current generation.
    pub fn apply_delta(&mut self, delta: RoutingDelta) -> Result<(), RoutingDeltaError> {
        if delta.base_generation != self.generation {
            return Err(RoutingDeltaError::GenerationMismatch {
                local: self.generation,
                base: delta.base_generation,
            });
        }
        for (bundle_id, owner) in delta.bundle_updates {
            match owner {
                Some(owner) => {
                    self.execution_bundle_owners.insert(bundle_id, owner);
                }
                None => {
                    self.execution_bundle_owners.remove(&bundle_id);
                }
            }
        }
        for (node_id, endpoint) in delta.node_updates {
            match endpoint {
                Some(endpoint) => {
                    self.node_endpoints.insert(node_id, endpoint);
                }
                None => {
                    self.node_endpoints.remove(&node_id);
                }
            }
        }
        self.generation = delta.generation;
        Ok(())
    }
}

/// Incremental routing update.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutingDelta {
    pub base_generation: GenerationCounter,
    pub bundle_updates: Vec<(BundleId, Option<BundleOwner>)>,
    pub node_updates: Vec<(IncarnationId, Option<NodeEndpoint>)>,
    pub generation: GenerationCounter,
}

/// Delta application failure.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum RoutingDeltaError {
    #[error("routing delta base generation {base:?} does not match local generation {local:?}")]
    GenerationMismatch {
        local: GenerationCounter,
        base: GenerationCounter,
    },
}

/// Deterministically map a placement key to a queue partition.
pub fn queue_partition_for(placement_key: &[u8], partition_count: u32) -> QueuePartition {
    assert!(partition_count > 0, "partition_count must be > 0");
    let hash = blake3::hash(placement_key);
    QueuePartition(first_hash_word(hash.as_bytes()) % partition_count)
}

/// Map a queue partition into a smaller bundle space.
pub fn bundle_for_partition(partition: QueuePartition, bundle_count: u32) -> BundleId {
    assert!(bundle_count > 0, "bundle_count must be > 0");
    ShardId(partition.0 % bundle_count)
}

/// Derive the correctness-home bundle from workflow identity.
pub fn execution_home_bundle(
    namespace_id: &[u8],
    workflow_id: &[u8],
    bundle_count: u32,
) -> BundleId {
    assert!(bundle_count > 0, "bundle_count must be > 0");
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"execution-home");
    hasher.update(namespace_id);
    hasher.update(workflow_id);
    let hash = hasher.finalize();
    ShardId(first_hash_word(hash.as_bytes()) % bundle_count)
}

fn first_hash_word(hash: &[u8; 32]) -> u32 {
    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(&hash[0..4]);
    u32::from_le_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use proptest::prelude::*;
    use uuid::Uuid;

    use super::*;
    use crate::{NamespaceId, TaskKind, TaskQueueName, WorkflowId};

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn queue_partition_is_deterministic(key in proptest::collection::vec(any::<u8>(), 0..128), count in 1u32..1024) {
            let first = queue_partition_for(&key, count);
            let second = queue_partition_for(&key, count);
            prop_assert_eq!(first, second);
            prop_assert!(first.0 < count);
        }

        #[test]
        fn bundle_for_partition_is_deterministic(partition in any::<u32>(), count in 1u32..1024) {
            let first = bundle_for_partition(QueuePartition(partition), count);
            let second = bundle_for_partition(QueuePartition(partition), count);
            prop_assert_eq!(first, second);
            prop_assert!(first.0 < count);
        }

        #[test]
        fn execution_home_is_deterministic(namespace in any::<[u8; 16]>(), workflow in proptest::collection::vec(any::<u8>(), 1..64), count in 1u32..1024) {
            let first = execution_home_bundle(&namespace, &workflow, count);
            let second = execution_home_bundle(&namespace, &workflow, count);
            prop_assert_eq!(first, second);
            prop_assert!(first.0 < count);
        }

        #[test]
        fn routing_delta_generation_is_monotonic(seed_generation in 0u64..u64::MAX - 1) {
            let placement_config = placement_config();
            let mut snapshot = RoutingSnapshot::new(placement_config);
            snapshot.generation = GenerationCounter(seed_generation);
            let node_id = IncarnationId::new();
            let next_generation = snapshot.generation.next();
            snapshot
                .apply_delta(RoutingDelta {
                    base_generation: GenerationCounter(seed_generation),
                    bundle_updates: vec![(
                        ShardId(1),
                        Some(BundleOwner {
                            node_id,
                            epoch: ShardEpoch(1),
                        }),
                    )],
                    node_updates: Vec::new(),
                    generation: next_generation,
                })
                .unwrap();
            prop_assert_eq!(snapshot.generation, next_generation);
            prop_assert!(snapshot.generation > GenerationCounter(seed_generation));
        }

        #[test]
        fn routing_delta_round_trip_enforces_base_generation(base in 0u64..u64::MAX - 1, stale in 0u64..u64::MAX - 1) {
            let mut snapshot = RoutingSnapshot::new(placement_config());
            snapshot.generation = GenerationCounter(base);
            let node_id = IncarnationId::new();
            let owner = BundleOwner {
                node_id,
                epoch: ShardEpoch(1),
            };
            let valid_delta = RoutingDelta {
                base_generation: GenerationCounter(base),
                bundle_updates: vec![(ShardId(1), Some(owner))],
                node_updates: Vec::new(),
                generation: GenerationCounter(base + 1),
            };
            let mut valid_snapshot = snapshot.clone();
            prop_assert!(valid_snapshot.apply_delta(valid_delta).is_ok());
            prop_assert_eq!(valid_snapshot.lookup_bundle_owner(ShardId(1)), Some(&owner));

            if stale != base {
                let invalid_delta = RoutingDelta {
                    base_generation: GenerationCounter(stale),
                    bundle_updates: Vec::new(),
                    node_updates: Vec::new(),
                    generation: GenerationCounter(base + 1),
                };
                let rejected = matches!(
                    snapshot.apply_delta(invalid_delta),
                    Err(RoutingDeltaError::GenerationMismatch { .. })
                );
                prop_assert!(rejected);
            }
        }
    }

    fn placement_config() -> PlacementConfig {
        PlacementConfig {
            shard_count: 4,
            bundle_count: 4,
            partition_count: 16,
            hash_version: 1,
        }
    }

    #[test]
    fn routing_snapshot_applies_matching_delta() {
        let placement_config = placement_config();
        let mut snapshot = RoutingSnapshot::new(placement_config);
        let node_id = IncarnationId::new();
        let owner = BundleOwner {
            node_id,
            epoch: ShardEpoch(7),
        };
        snapshot
            .apply_delta(RoutingDelta {
                base_generation: GenerationCounter::ZERO,
                bundle_updates: vec![(ShardId(1), Some(owner))],
                node_updates: vec![(
                    node_id,
                    Some(NodeEndpoint {
                        host: "127.0.0.1".to_owned(),
                        port: 7233,
                    }),
                )],
                generation: GenerationCounter(1),
            })
            .unwrap();

        assert_eq!(snapshot.lookup_bundle_owner(ShardId(1)), Some(&owner));
        assert_eq!(snapshot.generation, GenerationCounter(1));
    }

    #[test]
    fn routing_snapshot_rejects_mismatched_delta() {
        let placement_config = placement_config();
        let mut snapshot = RoutingSnapshot::new(placement_config);
        let err = snapshot
            .apply_delta(RoutingDelta {
                base_generation: GenerationCounter(9),
                bundle_updates: Vec::new(),
                node_updates: Vec::new(),
                generation: GenerationCounter(10),
            })
            .unwrap_err();
        assert!(matches!(err, RoutingDeltaError::GenerationMismatch { .. }));
    }

    #[test]
    fn queue_home_is_derived_from_bundle_owner() {
        let placement_config = placement_config();
        let node_id = IncarnationId::new();
        let mut owners = HashMap::new();
        owners.insert(
            ShardId(2),
            BundleOwner {
                node_id,
                epoch: ShardEpoch(3),
            },
        );
        let snapshot = RoutingSnapshot {
            execution_bundle_owners: owners,
            node_endpoints: HashMap::new(),
            placement_config,
            generation: GenerationCounter::ZERO,
        };
        let key = QueuePartitionKey {
            namespace_id: NamespaceId(Uuid::from_u128(1)),
            task_queue: TaskQueueName("q".to_owned()),
            task_kind: TaskKind::Workflow,
            partition: QueuePartition(6),
        };
        assert_eq!(
            snapshot.resolve_queue_home(&key).map(|owner| owner.node_id),
            Some(node_id)
        );
    }

    #[test]
    fn routing_snapshot_apply_delta_removes_entries() {
        let node_id = IncarnationId::new();
        let mut snapshot = RoutingSnapshot {
            execution_bundle_owners: HashMap::from([(
                ShardId(1),
                BundleOwner {
                    node_id,
                    epoch: ShardEpoch(3),
                },
            )]),
            node_endpoints: HashMap::from([(
                node_id,
                NodeEndpoint {
                    host: "127.0.0.1".to_owned(),
                    port: 7233,
                },
            )]),
            placement_config: placement_config(),
            generation: GenerationCounter(1),
        };

        snapshot
            .apply_delta(RoutingDelta {
                base_generation: GenerationCounter(1),
                bundle_updates: vec![(ShardId(1), None)],
                node_updates: vec![(node_id, None)],
                generation: GenerationCounter(2),
            })
            .unwrap();

        assert!(snapshot.lookup_bundle_owner(ShardId(1)).is_none());
        assert!(snapshot.lookup_node_endpoint(node_id).is_none());
        assert_eq!(snapshot.generation, GenerationCounter(2));
    }

    #[test]
    fn queue_partition_distribution_is_reasonably_uniform() {
        let partition_count = 16u32;
        let sample_count = 10_000u32;
        let expected = f64::from(sample_count) / f64::from(partition_count);
        let mut buckets = vec![0u32; partition_count as usize];
        for sample in 0..sample_count {
            let key = sample.to_le_bytes();
            let partition = queue_partition_for(&key, partition_count);
            buckets[partition.0 as usize] += 1;
        }
        let chi_squared = buckets
            .iter()
            .map(|count| {
                let delta = f64::from(*count) - expected;
                delta * delta / expected
            })
            .sum::<f64>();

        assert!(
            chi_squared < 40.0,
            "chi-squared statistic {chi_squared} exceeded uniformity threshold"
        );
    }

    #[test]
    fn execution_home_is_independent_of_operation_context() {
        let namespace_id = NamespaceId(Uuid::from_u128(42));
        let workflow_id = WorkflowId("wf".to_owned());
        let start_bundle =
            execution_home_bundle(namespace_id.0.as_bytes(), workflow_id.0.as_bytes(), 32);
        let signal_bundle =
            execution_home_bundle(namespace_id.0.as_bytes(), workflow_id.0.as_bytes(), 32);

        assert_eq!(start_bundle, signal_bundle);
    }
}
