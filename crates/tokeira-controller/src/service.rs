//! Shared placement-controller state and directive publication.
//!
//! Transport handling lives in `connect_service`; this module owns the stateful
//! operations used by both RPC handlers and the controller's periodic loops.
//! Keeping one served transport implementation prevents membership behavior
//! from diverging between a wired server and an unused protocol twin.

use std::sync::Arc;

use tokeira_observability::{ControllerCasOutcomeLabel, OutcomeLabel};
use tokeira_storage::{
    BudgetAllocationResult, ControlRepository, GenerationAdvanceResult, LeaseRepository,
};
use tokeira_types::{IncarnationId, PlacementConfig, RoutingSnapshot};
use tonic::Status;

use crate::{
    ControllerConfig, DrainCoordinator, GenerationManager, LiveMembership,
    membership::ControllerDirective,
    metrics,
    placement::{
        ConnectionBudgetDirective, DesiredPlacementDirective, compute_connection_budget,
        compute_desired_placement, compute_routing_snapshot, empty_previous_snapshot,
    },
};

/// Shared controller state used by the served Connect/gRPC implementation and
/// its periodic placement and budget loops.
#[derive(Clone)]
pub struct PlacementControllerState {
    pub config: ControllerConfig,
    pub leases: Arc<dyn LeaseRepository>,
    pub generation: GenerationManager,
    pub membership: Arc<tokio::sync::RwLock<LiveMembership>>,
    pub drain: Arc<tokio::sync::RwLock<DrainCoordinator>>,
}

impl std::fmt::Debug for PlacementControllerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlacementControllerState")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl PlacementControllerState {
    pub fn new(
        config: ControllerConfig,
        leases: Arc<dyn LeaseRepository>,
        control: Arc<dyn ControlRepository>,
    ) -> Self {
        Self {
            config,
            leases,
            generation: GenerationManager::new(control),
            membership: Arc::new(tokio::sync::RwLock::new(LiveMembership::default())),
            drain: Arc::new(tokio::sync::RwLock::new(DrainCoordinator::default())),
        }
    }

    pub(crate) async fn current_snapshot(&self) -> Result<RoutingSnapshot, Status> {
        let leases = self
            .leases
            .list_bundle_leases()
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        let previous = empty_previous_snapshot(self.placement_config());
        let (snapshot, delta) =
            compute_routing_snapshot(&leases, self.placement_config(), &previous);
        metrics::set_routing_snapshot_size(snapshot.execution_bundle_owners.len());
        metrics::record_bundle_ownership_churn(delta.bundle_updates.len());
        Ok(snapshot)
    }

    pub async fn advance_snapshot_generation(
        &self,
        expected: tokeira_types::GenerationCounter,
    ) -> Result<tokeira_types::GenerationCounter, Status> {
        let result = match self.generation.advance_generation(expected).await {
            Ok(result) => result,
            Err(err) => {
                metrics::record_generation_cas(ControllerCasOutcomeLabel::Error);
                return Err(Status::internal(err.to_string()));
            }
        };
        match result {
            GenerationAdvanceResult::Advanced(generation) => {
                metrics::record_generation_cas(ControllerCasOutcomeLabel::Success);
                Ok(generation)
            }
            GenerationAdvanceResult::Conflict(generation) => {
                metrics::record_generation_cas(ControllerCasOutcomeLabel::Conflict);
                Ok(generation)
            }
        }
    }

    pub async fn allocate_connection_budgets(
        &self,
        allocator_id: IncarnationId,
    ) -> Result<Vec<(IncarnationId, ConnectionBudgetDirective)>, Status> {
        let version = match self.generation.current_budget_version().await {
            Ok(version) => version,
            Err(err) => {
                metrics::record_budget_allocation(OutcomeLabel::Error);
                return Err(Status::internal(err.to_string()));
            }
        };
        let allocation = match self
            .generation
            .allocate_budget(
                version,
                allocator_id,
                self.config.dsql_connection_rate_budget,
                self.config.dsql_connection_capacity_budget,
            )
            .await
        {
            Ok(allocation) => allocation,
            Err(err) => {
                metrics::record_budget_allocation(OutcomeLabel::Error);
                return Err(Status::internal(err.to_string()));
            }
        };
        match allocation {
            BudgetAllocationResult::Allocated { .. } => {
                metrics::record_budget_allocation(OutcomeLabel::Success);
                let nodes = self.membership.read().await.active_node_ids_sorted();
                Ok(compute_connection_budget(
                    self.config.dsql_connection_rate_budget,
                    self.config.dsql_connection_capacity_budget,
                    &nodes,
                    self.config.budget_directive_validity,
                    self.config.dsql_connection_capacity_budget as u32,
                ))
            }
            BudgetAllocationResult::Conflict { .. } => {
                metrics::record_budget_allocation(OutcomeLabel::Conflict);
                Ok(Vec::new())
            }
        }
    }

    /// Compute desired placement from durable lease truth and publish one
    /// directive to every connected active runtime.
    ///
    /// The membership lock is released before awaiting channel capacity. A
    /// slow runtime stream therefore cannot stall heartbeat processing or node
    /// registration for the rest of the controller.
    pub async fn publish_desired_placements(&self) -> Result<usize, Status> {
        let leases = self
            .leases
            .list_bundle_leases()
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        let outbound = {
            let membership = self.membership.read().await;
            compute_desired_placement(&membership, &leases, self.config.bundle_count)
                .into_iter()
                .filter_map(|(node_id, directive)| {
                    membership
                        .directive_sender(node_id)
                        .map(|sender| (sender, ControllerDirective::DesiredPlacement(directive)))
                })
                .collect::<Vec<_>>()
        };
        Ok(send_directives(outbound).await)
    }

    /// Allocate the cluster-wide connection budget through the controller CAS
    /// and publish the resulting per-node shares to live runtime streams.
    pub async fn allocate_and_publish_connection_budgets(
        &self,
        allocator_id: IncarnationId,
    ) -> Result<usize, Status> {
        let budgets = self.allocate_connection_budgets(allocator_id).await?;
        let outbound = {
            let membership = self.membership.read().await;
            budgets
                .into_iter()
                .filter_map(|(node_id, directive)| {
                    membership
                        .directive_sender(node_id)
                        .map(|sender| (sender, ControllerDirective::ConnectionBudget(directive)))
                })
                .collect::<Vec<_>>()
        };
        Ok(send_directives(outbound).await)
    }

    /// Queue the required placement and connection-budget baseline for a newly
    /// registered runtime before its response stream is returned.
    pub(crate) async fn publish_initial_directives(
        &self,
        node_id: IncarnationId,
    ) -> Result<(), Status> {
        let leases = self
            .leases
            .list_bundle_leases()
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        let (sender, desired) = {
            let membership = self.membership.read().await;
            let sender = membership
                .directive_sender(node_id)
                .ok_or_else(|| Status::internal("membership directive stream is unavailable"))?;
            let desired = compute_desired_placement(&membership, &leases, self.config.bundle_count)
                .remove(&node_id)
                .unwrap_or_else(empty_desired_placement);
            (sender, desired)
        };
        let budget = self
            .allocate_connection_budgets(node_id)
            .await?
            .into_iter()
            .find_map(|(candidate, budget)| (candidate == node_id).then_some(budget))
            .ok_or_else(|| {
                Status::aborted(
                    "connection-budget allocation lost its CAS; retry the membership stream",
                )
            })?;

        sender
            .send(ControllerDirective::DesiredPlacement(desired))
            .await
            .map_err(|_| Status::internal("membership directive stream closed"))?;
        sender
            .send(ControllerDirective::ConnectionBudget(budget))
            .await
            .map_err(|_| Status::internal("membership directive stream closed"))?;
        Ok(())
    }

    fn placement_config(&self) -> PlacementConfig {
        PlacementConfig {
            shard_count: self.config.shard_count,
            bundle_count: self.config.bundle_count,
            partition_count: self.config.partition_count,
            hash_version: self.config.hash_version,
        }
    }

    /// Public accessor for the connect-rust service impl.
    pub(crate) fn placement_config_value(&self) -> PlacementConfig {
        self.placement_config()
    }
}

async fn send_directives(
    outbound: Vec<(
        tokio::sync::mpsc::Sender<ControllerDirective>,
        ControllerDirective,
    )>,
) -> usize {
    let mut delivered = 0;
    for (sender, directive) in outbound {
        if sender.send(directive).await.is_ok() {
            delivered += 1;
        }
    }
    delivered
}

fn empty_desired_placement() -> DesiredPlacementDirective {
    DesiredPlacementDirective {
        acquire_bundles: Vec::new(),
        relinquish_bundles: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use time::OffsetDateTime;
    use tokeira_storage::{ControlRepository, InMemoryStore, LeaseRepository};
    use tokeira_types::ShardId;

    use super::*;
    use crate::membership::{ControllerDirective, RuntimeHeartbeat};

    fn state() -> PlacementControllerState {
        let store = Arc::new(InMemoryStore::default());
        PlacementControllerState::new(
            ControllerConfig {
                bundle_count: 4,
                partition_count: 16,
                shard_count: 4,
                dsql_connection_rate_budget: 10.0,
                dsql_connection_capacity_budget: 11,
                ..ControllerConfig::default()
            },
            Arc::clone(&store) as Arc<dyn LeaseRepository>,
            store as Arc<dyn ControlRepository>,
        )
    }

    async fn register_with_directives(
        state: &PlacementControllerState,
        node_id: IncarnationId,
    ) -> tokio::sync::mpsc::Receiver<ControllerDirective> {
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        state.membership.write().await.register_node(
            crate::RuntimeRegistration {
                node_id,
                host: "127.0.0.1".to_owned(),
                port: 7233,
                zone: None,
                version: "v".to_owned(),
                build_id: "b".to_owned(),
            },
            RuntimeHeartbeat::empty(),
            Some(tx),
        );
        rx
    }

    #[tokio::test]
    async fn current_snapshot_returns_current_owner_with_epoch_and_endpoint() {
        let state = state();
        let node_id = IncarnationId::new();
        let epoch = match state
            .leases
            .try_acquire_bundle(ShardId(1), node_id.to_string(), "127.0.0.1:7233".to_owned())
            .await
            .unwrap()
        {
            tokeira_storage::LeaseOutcome::Acquired { epoch } => epoch,
            other => panic!("unexpected lease outcome: {other:?}"),
        };

        let snapshot = state.current_snapshot().await.unwrap();

        assert_eq!(
            snapshot.lookup_bundle_owner(ShardId(1)).copied(),
            Some(tokeira_types::BundleOwner { node_id, epoch })
        );
        let endpoint = snapshot
            .node_endpoints_iter()
            .find_map(|(candidate, endpoint)| (candidate == node_id).then_some(endpoint));
        assert_eq!(endpoint, Some(&"127.0.0.1:7233".parse().unwrap()));
    }

    #[tokio::test]
    async fn stream_open_queues_placement_then_budget() {
        let state = state();
        let node_id = IncarnationId::new();
        let mut rx = register_with_directives(&state, node_id).await;

        state.publish_initial_directives(node_id).await.unwrap();

        let ControllerDirective::DesiredPlacement(desired) = rx.recv().await.unwrap() else {
            panic!("placement must be the first stream directive");
        };
        assert_eq!(
            desired.acquire_bundles,
            vec![ShardId(0), ShardId(1), ShardId(2), ShardId(3)]
        );
        let ControllerDirective::ConnectionBudget(budget) = rx.recv().await.unwrap() else {
            panic!("budget must be the second stream directive");
        };
        assert_eq!(budget.rate_per_second, 10.0);
        assert_eq!(budget.capacity, 11);
    }

    #[tokio::test]
    async fn connection_budget_allocation_uses_cas_and_sorted_nodes() {
        let state = state();
        let node_a = IncarnationId::new();
        let node_b = IncarnationId::new();
        {
            let mut membership = state.membership.write().await;
            for node_id in [node_b, node_a] {
                membership.register_node(
                    crate::RuntimeRegistration {
                        node_id,
                        host: "127.0.0.1".to_owned(),
                        port: 7000,
                        zone: None,
                        version: "v".to_owned(),
                        build_id: "b".to_owned(),
                    },
                    RuntimeHeartbeat::empty(),
                    None,
                );
            }
        }

        let budgets = state.allocate_connection_budgets(node_a).await.unwrap();

        assert_eq!(budgets.len(), 2);
        assert_eq!(budgets[0].1.rate_per_second, 5.0);
        assert_eq!(budgets[0].1.capacity, 6);
        assert_eq!(budgets[1].1.capacity, 5);
        assert!(budgets[0].1.valid_until > OffsetDateTime::now_utc());
    }

    #[tokio::test]
    async fn periodic_loops_publish_fresh_placement_and_budgets() {
        let state = state();
        let node_id = IncarnationId::new();
        let mut rx = register_with_directives(&state, node_id).await;

        assert_eq!(state.publish_desired_placements().await.unwrap(), 1);
        assert!(matches!(
            rx.recv().await,
            Some(ControllerDirective::DesiredPlacement(_))
        ));

        assert_eq!(
            state
                .allocate_and_publish_connection_budgets(node_id)
                .await
                .unwrap(),
            1
        );
        assert!(matches!(
            rx.recv().await,
            Some(ControllerDirective::ConnectionBudget(_))
        ));
    }
}
