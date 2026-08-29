//! Controller service assembly types.
//!
//! The tonic transport implementation is intentionally kept as a thin layer
//! over these primitives so active-active behavior stays testable without a
//! running gRPC server.

use std::{pin::Pin, str::FromStr, sync::Arc};

use tokeira_observability::{ControllerCasOutcomeLabel, OutcomeLabel};
use tokeira_proto::controller::{
    self as proto, BundleOwnerMessage, BundleOwnershipEntry,
    ConnectionBudgetDirective as ProtoConnectionBudgetDirective,
    ControllerDirective as ProtoControllerDirective,
    DesiredPlacementDirective as ProtoDesiredPlacementDirective, FullRoutingSnapshot,
    MarkDrainingRequest, MarkDrainingResponse, NodeEndpointEntry, NodeEndpointMessage,
    NominateRequest, NominateResponse, PlacementConfigMessage, RefreshBundleRequest,
    RefreshBundleResponse, RoutingUpdate, ScaleInCandidate, SubscribeRoutingRequest,
    controller_directive::Directive, placement_controller_server::PlacementController,
    routing_update::Update, runtime_membership_request::Request as MembershipRequest,
};
use tokeira_storage::{
    BudgetAllocationResult, BundleLease, ControlRepository, GenerationAdvanceResult,
    LeaseRepository,
};
use tokeira_types::{
    BundleOwner, IncarnationId, NodeEndpoint, PlacementConfig, RoutingSnapshot, ShardId,
};
use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};
use tonic::{Request, Response, Status};

use crate::{
    ControllerConfig, DrainCoordinator, GenerationManager, LiveMembership,
    membership::{LanePressure, NodeDrainState, RuntimeHeartbeat, RuntimeRegistration},
    metrics,
    placement::{
        ConnectionBudgetDirective, compute_connection_budget, compute_desired_placement,
        compute_routing_snapshot, empty_previous_snapshot,
    },
};

/// Shared controller state used by the future tonic service.
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

#[tonic::async_trait]
impl PlacementController for PlacementControllerState {
    type RuntimeMembershipStream =
        Pin<Box<dyn Stream<Item = Result<ProtoControllerDirective, Status>> + Send + 'static>>;
    type SubscribeRoutingStream =
        Pin<Box<dyn Stream<Item = Result<RoutingUpdate, Status>> + Send + 'static>>;

    async fn runtime_membership(
        &self,
        request: Request<tonic::Streaming<proto::RuntimeMembershipRequest>>,
    ) -> Result<Response<Self::RuntimeMembershipStream>, Status> {
        let mut stream = request.into_inner();
        let Some(first) = stream.next().await else {
            return Err(Status::invalid_argument(
                "membership stream closed before registration",
            ));
        };
        let first = first?;
        let Some(MembershipRequest::Registration(registration)) = first.request else {
            return Err(Status::invalid_argument(
                "first membership message must be registration",
            ));
        };
        let registration = decode_registration(registration)?;
        let node_id = registration.node_id;
        {
            let mut membership = self.membership.write().await;
            membership.register_node(registration, RuntimeHeartbeat::empty(), None);
            metrics::set_membership_nodes_total(membership.nodes().count());
        }

        let state = self.clone();
        tokio::spawn(async move {
            while let Some(next) = stream.next().await {
                match next {
                    Ok(message) => {
                        if let Some(MembershipRequest::Heartbeat(heartbeat)) = message.request {
                            match decode_heartbeat(heartbeat) {
                                Ok(heartbeat) => {
                                    let mut drain = state.drain.write().await;
                                    drain.record_progress(node_id, heartbeat.drain_state);
                                    metrics::set_drain_active_nodes(drain.active_count());
                                    drop(drain);
                                    let mut membership = state.membership.write().await;
                                    membership.update_heartbeat(node_id, heartbeat);
                                    metrics::set_membership_nodes_total(membership.nodes().count());
                                }
                                Err(err) => {
                                    tracing::warn!(%err, "dropping invalid runtime heartbeat");
                                }
                            }
                        }
                    }
                    Err(err) => {
                        tracing::warn!(%err, "runtime membership stream failed");
                        break;
                    }
                }
            }
            let mut membership = state.membership.write().await;
            membership.mark_grace_period(node_id);
            metrics::set_membership_nodes_total(membership.nodes().count());
        });

        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let desired = {
            let membership = self.membership.read().await;
            let leases = self
                .leases
                .list_bundle_leases()
                .await
                .map_err(|err| Status::internal(err.to_string()))?;
            compute_desired_placement(&membership, &leases, self.config.bundle_count)
                .remove(&node_id)
        };
        if let Some(desired) = desired {
            let directive = ProtoControllerDirective {
                directive: Some(Directive::DesiredPlacement(
                    ProtoDesiredPlacementDirective {
                        acquire_bundles: desired
                            .acquire_bundles
                            .into_iter()
                            .map(|id| id.0)
                            .collect(),
                        relinquish_bundles: desired
                            .relinquish_bundles
                            .into_iter()
                            .map(|id| id.0)
                            .collect(),
                    },
                )),
            };
            tx.send(Ok(directive))
                .await
                .map_err(|_| Status::internal("membership directive stream closed"))?;
        }
        let budgets = self.allocate_connection_budgets(node_id).await?;
        if let Some((_node_id, budget)) = budgets.into_iter().find(|(id, _)| *id == node_id) {
            tx.send(Ok(ProtoControllerDirective {
                directive: Some(Directive::ConnectionBudget(encode_budget_directive(budget))),
            }))
            .await
            .map_err(|_| Status::internal("membership directive stream closed"))?;
        }
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn subscribe_routing(
        &self,
        _request: Request<SubscribeRoutingRequest>,
    ) -> Result<Response<Self::SubscribeRoutingStream>, Status> {
        let snapshot = self.current_snapshot().await?;
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tx.send(Ok(RoutingUpdate {
            update: Some(Update::Full(encode_snapshot(snapshot))),
        }))
        .await
        .map_err(|_| Status::internal("routing subscriber closed"))?;
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn refresh_bundle(
        &self,
        request: Request<RefreshBundleRequest>,
    ) -> Result<Response<RefreshBundleResponse>, Status> {
        let bundle_id = ShardId(request.into_inner().bundle_id);
        let leases = self
            .leases
            .list_bundle_leases()
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        let lease = leases
            .iter()
            .find(|lease| lease.bundle_id == bundle_id)
            .cloned();
        Ok(Response::new(refresh_response(bundle_id, lease)?))
    }

    async fn nominate_scale_in_candidates(
        &self,
        request: Request<NominateRequest>,
    ) -> Result<Response<NominateResponse>, Status> {
        let limit = request.into_inner().limit as usize;
        let membership = self.membership.read().await;
        let drain = self.drain.read().await;
        let mut nodes = membership
            .active_nodes()
            .filter(|node| !drain.is_draining(node.node_id))
            .map(|node| ScaleInCandidate {
                node_id: node.node_id.to_string(),
                owned_bundle_count: node.heartbeat.owned_bundle_count,
                runnable_transitions: node.heartbeat.runnable_transitions,
                active_actor_count: node.heartbeat.active_actor_count,
                backlog_depth: node.heartbeat.backlog_depth,
            })
            .collect::<Vec<_>>();
        nodes.sort_by_key(|node| {
            (
                node.owned_bundle_count,
                node.runnable_transitions,
                node.active_actor_count,
                node.backlog_depth,
            )
        });
        if limit > 0 {
            nodes.truncate(limit);
        }
        let aggregate_available_connections = membership
            .nodes()
            .map(|node| node.heartbeat.available_connections)
            .sum();
        let aggregate_connection_rate_headroom = membership
            .nodes()
            .map(|node| node.heartbeat.connection_rate_headroom)
            .sum();
        Ok(Response::new(NominateResponse {
            candidates: nodes,
            aggregate_available_connections,
            aggregate_connection_rate_headroom,
        }))
    }

    async fn mark_node_draining(
        &self,
        request: Request<MarkDrainingRequest>,
    ) -> Result<Response<MarkDrainingResponse>, Status> {
        let node_id = IncarnationId::from_str(&request.into_inner().node_id)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        self.membership.write().await.mark_draining(node_id);
        let mut drain = self.drain.write().await;
        drain.mark_draining(node_id);
        metrics::set_drain_active_nodes(drain.active_count());
        Ok(Response::new(MarkDrainingResponse { accepted: true }))
    }
}

fn decode_registration(
    registration: proto::RuntimeRegistration,
) -> Result<RuntimeRegistration, Status> {
    let node_id = IncarnationId::from_str(&registration.node_id)
        .map_err(|err| Status::invalid_argument(err.to_string()))?;
    let port = u16::try_from(registration.port)
        .map_err(|_| Status::invalid_argument("registration port exceeds u16"))?;
    Ok(RuntimeRegistration {
        node_id,
        host: registration.host,
        port,
        zone: (!registration.zone.is_empty()).then_some(registration.zone),
        version: registration.version,
        build_id: registration.build_id,
    })
}

fn decode_heartbeat(heartbeat: proto::RuntimeHeartbeat) -> Result<RuntimeHeartbeat, Status> {
    Ok(RuntimeHeartbeat {
        owned_bundle_count: heartbeat.owned_bundle_count,
        owned_bundles: heartbeat.owned_bundles.into_iter().map(ShardId).collect(),
        runnable_transitions: heartbeat.runnable_transitions,
        active_actor_count: heartbeat.active_actor_count,
        backlog_depth: heartbeat.backlog_depth,
        available_connections: heartbeat.available_connections,
        connection_rate_headroom: heartbeat.connection_rate_headroom,
        drain_state: match heartbeat.drain_state {
            value if value == proto::NodeDrainState::Draining as i32 => NodeDrainState::Draining,
            value if value == proto::NodeDrainState::SafeToTerminate as i32 => {
                NodeDrainState::SafeToTerminate
            }
            _ => NodeDrainState::Active,
        },
        lane_pressures: heartbeat
            .lane_pressures
            .into_iter()
            .map(|lane| LanePressure {
                lane_id: lane.lane_id,
                runnable_depth: lane.runnable_depth,
                active_actors: lane.active_actors,
                utilization: lane.utilization,
            })
            .collect(),
    })
}

fn encode_snapshot(snapshot: RoutingSnapshot) -> FullRoutingSnapshot {
    FullRoutingSnapshot {
        bundles: snapshot
            .execution_bundle_owners
            .into_iter()
            .map(|(bundle_id, owner)| encode_bundle_owner(bundle_id, Some(owner)))
            .collect(),
        nodes: snapshot
            .node_endpoints
            .into_iter()
            .map(|(node_id, endpoint)| encode_node_endpoint(node_id, Some(endpoint)))
            .collect(),
        placement_config: Some(PlacementConfigMessage {
            shard_count: snapshot.placement_config.shard_count,
            bundle_count: snapshot.placement_config.bundle_count,
            partition_count: snapshot.placement_config.partition_count,
            hash_version: snapshot.placement_config.hash_version,
        }),
        generation: snapshot.generation.0,
    }
}

fn encode_bundle_owner(bundle_id: ShardId, owner: Option<BundleOwner>) -> BundleOwnershipEntry {
    BundleOwnershipEntry {
        bundle_id: bundle_id.0,
        state: Some(match owner {
            Some(owner) => proto::bundle_ownership_entry::State::Owner(BundleOwnerMessage {
                owner_node_id: owner.node_id.to_string(),
                epoch: owner.epoch.0,
            }),
            None => proto::bundle_ownership_entry::State::Unowned(true),
        }),
    }
}

fn encode_node_endpoint(
    node_id: IncarnationId,
    endpoint: Option<NodeEndpoint>,
) -> NodeEndpointEntry {
    NodeEndpointEntry {
        node_id: node_id.to_string(),
        state: Some(match endpoint {
            Some(endpoint) => proto::node_endpoint_entry::State::Endpoint(NodeEndpointMessage {
                host: endpoint.host,
                port: u32::from(endpoint.port),
            }),
            None => proto::node_endpoint_entry::State::Removed(true),
        }),
    }
}

fn encode_budget_directive(budget: ConnectionBudgetDirective) -> ProtoConnectionBudgetDirective {
    ProtoConnectionBudgetDirective {
        rate_per_second: budget.rate_per_second,
        capacity: budget.capacity,
        max_reservoir_size: budget.max_reservoir_size,
        valid_until: Some(prost_types::Timestamp {
            seconds: budget.valid_until.unix_timestamp(),
            nanos: budget.valid_until.nanosecond() as i32,
        }),
    }
}

fn refresh_response(
    bundle_id: ShardId,
    lease: Option<BundleLease>,
) -> Result<RefreshBundleResponse, Status> {
    let Some(lease) = lease else {
        return Ok(RefreshBundleResponse {
            bundle: Some(encode_bundle_owner(bundle_id, None)),
            node: None,
        });
    };
    let owner = lease
        .owner_node_id
        .as_deref()
        .map(IncarnationId::from_str)
        .transpose()
        .map_err(|err| Status::internal(err.to_string()))?;
    let endpoint = lease
        .node_endpoint
        .as_deref()
        .map(NodeEndpoint::from_str)
        .transpose()
        .map_err(|err| Status::internal(err.to_string()))?;
    let bundle = encode_bundle_owner(
        bundle_id,
        owner.map(|node_id| BundleOwner {
            node_id,
            epoch: lease.epoch,
        }),
    );
    let node = match (owner, endpoint) {
        (Some(node_id), endpoint) => Some(encode_node_endpoint(node_id, endpoint)),
        _ => None,
    };
    Ok(RefreshBundleResponse {
        bundle: Some(bundle),
        node,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use time::OffsetDateTime;
    use tokeira_storage::{ControlRepository, InMemoryStore, LeaseRepository};

    use super::*;
    use crate::membership::{NodeMembershipState, RuntimeHeartbeat};

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

    #[tokio::test]
    async fn refresh_bundle_returns_current_owner_with_epoch_and_endpoint() {
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

        let response = state
            .refresh_bundle(Request::new(RefreshBundleRequest { bundle_id: 1 }))
            .await
            .unwrap()
            .into_inner();

        let bundle = response.bundle.unwrap();
        let Some(proto::bundle_ownership_entry::State::Owner(owner)) = bundle.state else {
            panic!("expected owned bundle");
        };
        assert_eq!(owner.owner_node_id, node_id.to_string());
        assert_eq!(owner.epoch, epoch.0);
        let node = response.node.unwrap();
        let Some(proto::node_endpoint_entry::State::Endpoint(endpoint)) = node.state else {
            panic!("expected endpoint");
        };
        assert_eq!(endpoint.host, "127.0.0.1");
        assert_eq!(endpoint.port, 7233);
    }

    #[tokio::test]
    async fn scale_in_candidates_exclude_draining_and_report_headroom() {
        let state = state();
        let draining = IncarnationId::new();
        let healthy = IncarnationId::new();
        {
            let mut membership = state.membership.write().await;
            membership.register_node(
                crate::RuntimeRegistration {
                    node_id: draining,
                    host: "127.0.0.1".to_owned(),
                    port: 7233,
                    zone: None,
                    version: "v".to_owned(),
                    build_id: "b".to_owned(),
                },
                RuntimeHeartbeat {
                    owned_bundle_count: 2,
                    available_connections: 3,
                    connection_rate_headroom: 1.5,
                    ..RuntimeHeartbeat::empty()
                },
                None,
            );
            membership.register_node(
                crate::RuntimeRegistration {
                    node_id: healthy,
                    host: "127.0.0.1".to_owned(),
                    port: 7234,
                    zone: None,
                    version: "v".to_owned(),
                    build_id: "b".to_owned(),
                },
                RuntimeHeartbeat {
                    owned_bundle_count: 1,
                    available_connections: 5,
                    connection_rate_headroom: 2.5,
                    ..RuntimeHeartbeat::empty()
                },
                None,
            );
        }
        state.drain.write().await.mark_draining(draining);

        let response = state
            .nominate_scale_in_candidates(Request::new(NominateRequest { limit: 10 }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.candidates.len(), 1);
        assert_eq!(response.candidates[0].node_id, healthy.to_string());
        assert_eq!(response.aggregate_available_connections, 8);
        assert_eq!(response.aggregate_connection_rate_headroom, 4.0);
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
    async fn mark_node_draining_updates_membership_and_drain_state() {
        let state = state();
        let node_id = IncarnationId::new();
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
            None,
        );

        state
            .mark_node_draining(Request::new(MarkDrainingRequest {
                node_id: node_id.to_string(),
            }))
            .await
            .unwrap();

        assert_eq!(
            state
                .membership
                .read()
                .await
                .get(node_id)
                .unwrap()
                .membership_state,
            NodeMembershipState::Draining
        );
        assert!(state.drain.read().await.is_draining(node_id));
    }
}
