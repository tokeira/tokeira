//! Connect-rust service implementation for the placement controller.
//!
//! Wraps `PlacementControllerState` and implements the connect-rust
//! `PlacementController` trait. Provides zero-copy request views and
//! serves Connect, gRPC, and gRPC-Web on the same handlers.

use connectrpc::{RequestContext, Response, ServiceResult, ServiceStream};
use futures::StreamExt;
use tokeira_proto::connect::tokeira::internal::controller::v1::{
    self as pb, BundleOwnerMessage, BundleOwnershipEntry,
    ControllerDirective as WireControllerDirective, DescribeNodeDrainResponse, FullRoutingSnapshot,
    MarkDrainingResponse, NodeEndpointEntry, NodeEndpointMessage, NominateResponse,
    OwnedDescribeNodeDrainRequestView, OwnedMarkDrainingRequestView, OwnedNominateRequestView,
    OwnedRefreshBundleRequestView, OwnedRuntimeMembershipRequestView,
    OwnedSubscribeRoutingRequestView, PlacementConfigMessage, PlacementController,
    RefreshBundleResponse, RoutingUpdate, ScaleInCandidate, bundle_ownership_entry,
    node_endpoint_entry, routing_update,
};
use tokeira_types::{IncarnationId, PlacementConfig, ShardId};

use crate::{
    PlacementControllerState,
    membership::{ControllerDirective, NodeDrainState, RuntimeHeartbeat, RuntimeRegistration},
    metrics,
};

/// Connect-rust service wrapper around the shared controller state.
#[derive(Debug)]
pub struct ConnectPlacementController {
    pub(crate) state: PlacementControllerState,
}

impl ConnectPlacementController {
    pub fn new(state: PlacementControllerState) -> Self {
        Self { state }
    }
}

impl PlacementController for ConnectPlacementController {
    async fn runtime_membership(
        &self,
        _ctx: RequestContext,
        requests: ServiceStream<OwnedRuntimeMembershipRequestView>,
    ) -> ServiceResult<ServiceStream<WireControllerDirective>> {
        use pb::runtime_membership_request::Request;

        let mut stream = requests;
        let Some(first) = stream.next().await else {
            return Err(connectrpc::ConnectError::new(
                connectrpc::ErrorCode::InvalidArgument,
                "membership stream closed before registration",
            ));
        };
        let first = first?;
        let Some(Request::Registration(registration)) = first.to_owned_message().request else {
            return Err(connectrpc::ConnectError::new(
                connectrpc::ErrorCode::InvalidArgument,
                "first membership message must be registration",
            ));
        };
        let registration = decode_registration(*registration)?;
        let node_id = registration.node_id;
        let (tx, rx) = tokio::sync::mpsc::channel::<ControllerDirective>(16);
        {
            let mut membership = self.state.membership.write().await;
            membership.register_node(registration, RuntimeHeartbeat::empty(), Some(tx.clone()));
            metrics::set_membership_nodes_total(membership.nodes().count());
        }
        if let Err(error) = self.state.publish_initial_directives(node_id).await {
            self.state
                .membership
                .write()
                .await
                .mark_grace_period_for_stream(node_id, &tx);
            return Err(connectrpc::ConnectError::internal(error.to_string()));
        }

        let state = self.state.clone();
        let stream_tx = tx.clone();
        tokio::spawn(async move {
            while let Some(msg) = stream.next().await {
                let Ok(msg) = msg else {
                    break;
                };
                let owned = msg.to_owned_message();

                match owned.request {
                    Some(Request::Heartbeat(hb)) => {
                        let heartbeat = decode_heartbeat(&hb);
                        let mut drain = state.drain.write().await;
                        drain.record_progress(node_id, heartbeat.drain_state);
                        metrics::set_drain_active_nodes(drain.active_count());
                        drop(drain);
                        let mut membership = state.membership.write().await;
                        membership.update_heartbeat(node_id, heartbeat);
                        metrics::set_membership_nodes_total(membership.nodes().count());
                    }
                    // Registration is a stream-opening frame. Re-registration
                    // uses a new stream so its response channel and disconnect
                    // fencing are replaced atomically in LiveMembership.
                    Some(Request::Registration(_)) => {}
                    None => {}
                }
            }
            let mut membership = state.membership.write().await;
            membership.mark_grace_period_for_stream(node_id, &stream_tx);
            metrics::set_membership_nodes_total(membership.nodes().count());
        });

        let response_stream = tokio_stream::wrappers::ReceiverStream::new(rx)
            .map(|directive| Ok(encode_controller_directive(directive)));
        Response::stream_ok(response_stream)
    }

    async fn subscribe_routing(
        &self,
        _ctx: RequestContext,
        _req: OwnedSubscribeRoutingRequestView,
    ) -> ServiceResult<ServiceStream<RoutingUpdate>> {
        let snapshot = self
            .state
            .current_snapshot()
            .await
            .map_err(|e| connectrpc::ConnectError::internal(e.to_string()))?;
        let config = self.state.placement_config_value();
        let update = encode_routing_update(&snapshot, &config);
        let s = futures::stream::iter(vec![Ok(update)]);
        Response::stream_ok(s)
    }

    async fn refresh_bundle(
        &self,
        _ctx: RequestContext,
        req: OwnedRefreshBundleRequestView,
    ) -> ServiceResult<RefreshBundleResponse> {
        let bundle_id = ShardId(req.bundle_id);
        let leases = self
            .state
            .leases
            .list_bundle_leases()
            .await
            .map_err(|e| connectrpc::ConnectError::internal(e.to_string()))?;

        let lease = leases.iter().find(|l| l.bundle_id == bundle_id);
        let (bundle_entry, node_entry) = match lease {
            Some(l) => encode_lease_entry(l),
            None => (
                BundleOwnershipEntry {
                    bundle_id: bundle_id.0,
                    state: Some(bundle_ownership_entry::State::Unowned(true)),
                    ..Default::default()
                },
                NodeEndpointEntry::default(),
            ),
        };

        Response::ok(RefreshBundleResponse {
            bundle: bundle_entry.into(),
            node: node_entry.into(),
            ..Default::default()
        })
    }

    async fn nominate_scale_in_candidates(
        &self,
        _ctx: RequestContext,
        req: OwnedNominateRequestView,
    ) -> ServiceResult<NominateResponse> {
        let membership = self.state.membership.read().await;
        let candidates = membership.nominate_scale_in(req.limit);
        let (agg_connections, agg_rate) = membership.aggregate_headroom();

        Response::ok(NominateResponse {
            candidates: candidates
                .into_iter()
                .map(|c| ScaleInCandidate {
                    node_id: c.node_id.0.to_string(),
                    owned_bundle_count: c.owned_bundle_count,
                    runnable_transitions: c.runnable_transitions,
                    active_actor_count: c.active_actor_count,
                    backlog_depth: c.backlog_depth,
                    ..Default::default()
                })
                .collect(),
            aggregate_available_connections: agg_connections,
            aggregate_connection_rate_headroom: agg_rate,
            ..Default::default()
        })
    }

    async fn mark_node_draining(
        &self,
        _ctx: RequestContext,
        req: OwnedMarkDrainingRequestView,
    ) -> ServiceResult<MarkDrainingResponse> {
        let node_id = parse_node_id(req.node_id)?;
        let accepted = self.state.mark_node_draining(node_id).await;
        Response::ok(MarkDrainingResponse {
            accepted,
            ..Default::default()
        })
    }

    async fn describe_node_drain(
        &self,
        _ctx: RequestContext,
        req: OwnedDescribeNodeDrainRequestView,
    ) -> ServiceResult<DescribeNodeDrainResponse> {
        let node_id = parse_node_id(req.node_id)?;
        let state = self.state.describe_node_drain(node_id).await;
        Response::ok(DescribeNodeDrainResponse {
            known: state.is_some(),
            state: buffa::EnumValue::Known(encode_drain_state(state)),
            ..Default::default()
        })
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn parse_node_id(node_id: &str) -> Result<IncarnationId, connectrpc::ConnectError> {
    node_id
        .parse::<uuid::Uuid>()
        .map(IncarnationId)
        .map_err(|_| {
            connectrpc::ConnectError::new(
                connectrpc::ErrorCode::InvalidArgument,
                "invalid node_id UUID",
            )
        })
}

fn encode_drain_state(state: Option<NodeDrainState>) -> pb::NodeDrainState {
    match state {
        None => pb::NodeDrainState::NODE_DRAIN_STATE_UNSPECIFIED,
        Some(NodeDrainState::Active) => pb::NodeDrainState::NODE_DRAIN_STATE_ACTIVE,
        Some(NodeDrainState::Draining) => pb::NodeDrainState::NODE_DRAIN_STATE_DRAINING,
        Some(NodeDrainState::SafeToTerminate) => {
            pb::NodeDrainState::NODE_DRAIN_STATE_SAFE_TO_TERMINATE
        }
    }
}

fn decode_heartbeat(hb: &pb::RuntimeHeartbeat) -> RuntimeHeartbeat {
    use buffa::EnumValue;
    use pb::NodeDrainState as PbDrainState;

    let drain_state = match &hb.drain_state {
        EnumValue::Known(PbDrainState::NODE_DRAIN_STATE_ACTIVE) => NodeDrainState::Active,
        EnumValue::Known(PbDrainState::NODE_DRAIN_STATE_DRAINING) => NodeDrainState::Draining,
        EnumValue::Known(PbDrainState::NODE_DRAIN_STATE_SAFE_TO_TERMINATE) => {
            NodeDrainState::SafeToTerminate
        }
        _ => NodeDrainState::Active,
    };
    RuntimeHeartbeat {
        owned_bundle_count: hb.owned_bundle_count,
        owned_bundles: hb.owned_bundles.iter().copied().map(ShardId).collect(),
        runnable_transitions: hb.runnable_transitions,
        active_actor_count: hb.active_actor_count,
        backlog_depth: hb.backlog_depth,
        available_connections: hb.available_connections,
        connection_rate_headroom: hb.connection_rate_headroom,
        drain_state,
        lane_pressures: hb
            .lane_pressures
            .iter()
            .map(|lp| crate::membership::LanePressure {
                lane_id: lp.lane_id,
                runnable_depth: lp.runnable_depth,
                active_actors: lp.active_actors,
                utilization: lp.utilization,
            })
            .collect(),
    }
}

fn decode_registration(
    registration: pb::RuntimeRegistration,
) -> Result<RuntimeRegistration, connectrpc::ConnectError> {
    let node_id = registration
        .node_id
        .parse::<uuid::Uuid>()
        .map(IncarnationId)
        .map_err(|_| {
            connectrpc::ConnectError::new(
                connectrpc::ErrorCode::InvalidArgument,
                "registration node_id must be a UUID",
            )
        })?;
    let port = u16::try_from(registration.port).map_err(|_| {
        connectrpc::ConnectError::new(
            connectrpc::ErrorCode::InvalidArgument,
            "registration port exceeds u16",
        )
    })?;
    Ok(RuntimeRegistration {
        node_id,
        host: registration.host,
        port,
        zone: (!registration.zone.is_empty()).then_some(registration.zone),
        version: registration.version,
        build_id: registration.build_id,
    })
}

fn encode_controller_directive(directive: ControllerDirective) -> WireControllerDirective {
    use pb::controller_directive::Directive;

    let directive = match directive {
        ControllerDirective::DesiredPlacement(desired) => Directive::DesiredPlacement(
            pb::DesiredPlacementDirective {
                acquire_bundles: desired
                    .acquire_bundles
                    .into_iter()
                    .map(|bundle| bundle.0)
                    .collect(),
                relinquish_bundles: desired
                    .relinquish_bundles
                    .into_iter()
                    .map(|bundle| bundle.0)
                    .collect(),
                ..Default::default()
            }
            .into(),
        ),
        ControllerDirective::ConnectionBudget(budget) => {
            let mut encoded = pb::ConnectionBudgetDirective {
                rate_per_second: budget.rate_per_second,
                capacity: budget.capacity,
                max_reservoir_size: budget.max_reservoir_size,
                ..Default::default()
            };
            let valid_until = encoded.valid_until.get_or_insert_default();
            valid_until.seconds = budget.valid_until.unix_timestamp();
            valid_until.nanos = budget.valid_until.nanosecond() as i32;
            Directive::ConnectionBudget(encoded.into())
        }
        ControllerDirective::Drain => Directive::Drain(pb::DrainDirective::default().into()),
    };
    WireControllerDirective {
        directive: Some(directive),
        ..Default::default()
    }
}

fn encode_routing_update(
    snapshot: &tokeira_types::RoutingSnapshot,
    config: &PlacementConfig,
) -> RoutingUpdate {
    let bundles = snapshot
        .bundle_owners()
        .map(|(id, owner)| BundleOwnershipEntry {
            bundle_id: id.0,
            state: Some(bundle_ownership_entry::State::Owner(
                BundleOwnerMessage {
                    owner_node_id: owner.node_id.0.to_string(),
                    epoch: owner.epoch.0,
                    ..Default::default()
                }
                .into(),
            )),
            ..Default::default()
        })
        .collect();

    let nodes = snapshot
        .node_endpoints_iter()
        .map(|(id, ep)| NodeEndpointEntry {
            node_id: id.0.to_string(),
            state: Some(node_endpoint_entry::State::Endpoint(
                NodeEndpointMessage {
                    host: ep.host.clone(),
                    port: ep.port as u32,
                    ..Default::default()
                }
                .into(),
            )),
            ..Default::default()
        })
        .collect();

    RoutingUpdate {
        update: Some(routing_update::Update::Full(
            FullRoutingSnapshot {
                bundles,
                nodes,
                placement_config: PlacementConfigMessage {
                    shard_count: config.shard_count,
                    bundle_count: config.bundle_count,
                    partition_count: config.partition_count,
                    hash_version: config.hash_version,
                    ..Default::default()
                }
                .into(),
                generation: snapshot.generation.0,
                ..Default::default()
            }
            .into(),
        )),
        ..Default::default()
    }
}

fn encode_lease_entry(
    lease: &tokeira_storage::BundleLease,
) -> (BundleOwnershipEntry, NodeEndpointEntry) {
    let bundle = match &lease.owner_node_id {
        Some(owner) => BundleOwnershipEntry {
            bundle_id: lease.bundle_id.0,
            state: Some(bundle_ownership_entry::State::Owner(
                BundleOwnerMessage {
                    owner_node_id: owner.clone(),
                    epoch: lease.epoch.0,
                    ..Default::default()
                }
                .into(),
            )),
            ..Default::default()
        },
        None => BundleOwnershipEntry {
            bundle_id: lease.bundle_id.0,
            state: Some(bundle_ownership_entry::State::Unowned(true)),
            ..Default::default()
        },
    };

    let node = match &lease.node_endpoint {
        Some(ep) => {
            let (host, port) = ep.split_once(':').unwrap_or((ep, "0"));
            NodeEndpointEntry {
                node_id: lease.owner_node_id.clone().unwrap_or_default(),
                state: Some(node_endpoint_entry::State::Endpoint(
                    NodeEndpointMessage {
                        host: host.to_owned(),
                        port: port.parse().unwrap_or(0),
                        ..Default::default()
                    }
                    .into(),
                )),
                ..Default::default()
            }
        }
        None => NodeEndpointEntry::default(),
    };

    (bundle, node)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::{StreamExt, stream};
    use pb::{controller_directive, runtime_membership_request};
    use tokeira_storage::{ControlRepository, InMemoryStore, LeaseOutcome, LeaseRepository};
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    use super::*;
    use crate::ControllerConfig;

    fn service() -> ConnectPlacementController {
        service_with_store(Arc::new(InMemoryStore::default()))
    }

    fn service_with_store(store: Arc<InMemoryStore>) -> ConnectPlacementController {
        ConnectPlacementController::new(PlacementControllerState::new(
            ControllerConfig {
                bundle_count: 1,
                shard_count: 1,
                dsql_connection_rate_budget: 10.0,
                dsql_connection_capacity_budget: 10,
                ..ControllerConfig::default()
            },
            Arc::clone(&store) as Arc<dyn LeaseRepository>,
            store as Arc<dyn ControlRepository>,
        ))
    }

    fn registration_request(node_id: IncarnationId) -> OwnedRuntimeMembershipRequestView {
        let request = pb::RuntimeMembershipRequest {
            request: Some(runtime_membership_request::Request::Registration(
                pb::RuntimeRegistration {
                    node_id: node_id.to_string(),
                    host: "127.0.0.1".to_owned(),
                    port: 7233,
                    version: "test".to_owned(),
                    build_id: "test".to_owned(),
                    ..Default::default()
                }
                .into(),
            )),
            ..Default::default()
        };
        OwnedRuntimeMembershipRequestView::from_owned(&request).unwrap()
    }

    fn heartbeat_request(drain_state: pb::NodeDrainState) -> OwnedRuntimeMembershipRequestView {
        let request = pb::RuntimeMembershipRequest {
            request: Some(runtime_membership_request::Request::Heartbeat(
                pb::RuntimeHeartbeat {
                    drain_state: buffa::EnumValue::Known(drain_state),
                    ..Default::default()
                }
                .into(),
            )),
            ..Default::default()
        };
        OwnedRuntimeMembershipRequestView::from_owned(&request).unwrap()
    }

    /// A request stream that stays open and can be fed heartbeats after the
    /// registration frame, standing in for a live runtime.
    fn open_requests(
        node_id: IncarnationId,
    ) -> (
        mpsc::Sender<Result<OwnedRuntimeMembershipRequestView, connectrpc::ConnectError>>,
        ServiceStream<OwnedRuntimeMembershipRequestView>,
    ) {
        let (tx, rx) = mpsc::channel(8);
        tx.try_send(Ok(registration_request(node_id)))
            .expect("fresh channel accepts the registration frame");
        (tx, Box::pin(ReceiverStream::new(rx)))
    }

    fn mark_request(node_id: IncarnationId) -> OwnedMarkDrainingRequestView {
        OwnedMarkDrainingRequestView::from_owned(&pb::MarkDrainingRequest {
            node_id: node_id.to_string(),
            ..Default::default()
        })
        .unwrap()
    }

    async fn describe(
        service: &ConnectPlacementController,
        node_id: IncarnationId,
    ) -> (bool, pb::NodeDrainState) {
        let request =
            OwnedDescribeNodeDrainRequestView::from_owned(&pb::DescribeNodeDrainRequest {
                node_id: node_id.to_string(),
                ..Default::default()
            })
            .unwrap();
        let response = service
            .describe_node_drain(RequestContext::default(), request)
            .await
            .unwrap()
            .body;
        let buffa::EnumValue::Known(state) = response.state else {
            panic!("drain state must be a known enum value");
        };
        (response.known, state)
    }

    /// Yield to the served stream's reader task until `condition` holds.
    async fn wait_until(mut condition: impl AsyncFnMut() -> bool) {
        for _ in 0..10_000 {
            if condition().await {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("condition was not reached");
    }

    #[tokio::test]
    async fn served_membership_stream_delivers_initial_and_loop_directives() {
        let service = service();
        let node_id = IncarnationId::new();
        let request = registration_request(node_id);
        // Keep the request side open: closing it represents a disconnected
        // runtime and correctly moves the node out of active placement.
        let requests: ServiceStream<OwnedRuntimeMembershipRequestView> =
            Box::pin(stream::once(async move { Ok(request) }).chain(stream::pending()));

        let mut directives = service
            .runtime_membership(RequestContext::default(), requests)
            .await
            .unwrap()
            .body;

        let first = directives.next().await.unwrap().unwrap();
        let Some(controller_directive::Directive::DesiredPlacement(desired)) = first.directive
        else {
            panic!("first directive must be desired placement");
        };
        assert_eq!(desired.acquire_bundles, vec![0]);
        assert!(matches!(
            directives.next().await.unwrap().unwrap().directive,
            Some(controller_directive::Directive::ConnectionBudget(_))
        ));

        assert_eq!(service.state.publish_desired_placements().await.unwrap(), 1);
        assert!(matches!(
            directives.next().await.unwrap().unwrap().directive,
            Some(controller_directive::Directive::DesiredPlacement(_))
        ));
        assert_eq!(
            service
                .state
                .allocate_and_publish_connection_budgets(node_id)
                .await
                .unwrap(),
            1
        );
        assert!(matches!(
            directives.next().await.unwrap().unwrap().directive,
            Some(controller_directive::Directive::ConnectionBudget(_))
        ));
    }

    #[tokio::test]
    async fn served_drain_reaches_the_stream_and_describe_follows_the_heartbeat() {
        let store = Arc::new(InMemoryStore::default());
        let service = service_with_store(Arc::clone(&store));
        let draining = IncarnationId::new();
        let (requests_tx, requests) = open_requests(draining);
        let mut directives = service
            .runtime_membership(RequestContext::default(), requests)
            .await
            .unwrap()
            .body;
        assert!(matches!(
            directives.next().await.unwrap().unwrap().directive,
            Some(controller_directive::Directive::DesiredPlacement(_))
        ));
        assert!(matches!(
            directives.next().await.unwrap().unwrap().directive,
            Some(controller_directive::Directive::ConnectionBudget(_))
        ));
        // The runtime enacted its placement: it holds the durable lease.
        let epoch = match store
            .try_acquire_bundle(
                ShardId(0),
                draining.to_string(),
                "127.0.0.1:7233".to_owned(),
            )
            .await
            .unwrap()
        {
            LeaseOutcome::Acquired { epoch } => epoch,
            other => panic!("unexpected lease outcome: {other:?}"),
        };

        let accepted = service
            .mark_node_draining(RequestContext::default(), mark_request(draining))
            .await
            .unwrap()
            .body
            .accepted;
        assert!(accepted);
        assert!(matches!(
            directives.next().await.unwrap().unwrap().directive,
            Some(controller_directive::Directive::Drain(_))
        ));
        assert_eq!(
            describe(&service, draining).await,
            (true, pb::NodeDrainState::NODE_DRAIN_STATE_DRAINING)
        );

        // Progress arrives only through the runtime's heartbeat.
        requests_tx
            .send(Ok(heartbeat_request(
                pb::NodeDrainState::NODE_DRAIN_STATE_SAFE_TO_TERMINATE,
            )))
            .await
            .unwrap();
        wait_until(async || {
            describe(&service, draining).await.1
                == pb::NodeDrainState::NODE_DRAIN_STATE_SAFE_TO_TERMINATE
        })
        .await;

        // A repeated mark is accepted and never demotes the verdict.
        assert!(
            service
                .mark_node_draining(RequestContext::default(), mark_request(draining))
                .await
                .unwrap()
                .body
                .accepted
        );
        assert_eq!(
            describe(&service, draining).await,
            (true, pb::NodeDrainState::NODE_DRAIN_STATE_SAFE_TO_TERMINATE)
        );

        // Once the runtime relinquishes, placement hands the bundle to an
        // active node and never back to the draining one.
        assert!(matches!(
            store
                .relinquish_bundle(ShardId(0), draining.to_string(), epoch)
                .await
                .unwrap(),
            LeaseOutcome::Acquired { .. }
        ));
        let successor = IncarnationId::new();
        let (_successor_tx, successor_requests) = open_requests(successor);
        let mut successor_directives = service
            .runtime_membership(RequestContext::default(), successor_requests)
            .await
            .unwrap()
            .body;
        let Some(controller_directive::Directive::DesiredPlacement(desired)) = successor_directives
            .next()
            .await
            .unwrap()
            .unwrap()
            .directive
        else {
            panic!("successor's first directive must be desired placement");
        };
        assert_eq!(desired.acquire_bundles, vec![0]);
        assert_eq!(service.state.publish_desired_placements().await.unwrap(), 1);
        assert!(matches!(
            successor_directives
                .next()
                .await
                .unwrap()
                .unwrap()
                .directive,
            Some(controller_directive::Directive::ConnectionBudget(_))
        ));
    }

    #[tokio::test]
    async fn reopened_stream_of_a_draining_node_receives_the_drain_again() {
        let service = service();
        let node_id = IncarnationId::new();
        let (first_tx, first_requests) = open_requests(node_id);
        let mut first = service
            .runtime_membership(RequestContext::default(), first_requests)
            .await
            .unwrap()
            .body;
        first.next().await.unwrap().unwrap();
        first.next().await.unwrap().unwrap();
        assert!(service.state.mark_node_draining(node_id).await);
        assert!(matches!(
            first.next().await.unwrap().unwrap().directive,
            Some(controller_directive::Directive::Drain(_))
        ));

        // The stream drops mid-drain and the same incarnation reconnects.
        drop(first_tx);
        let (_second_tx, second_requests) = open_requests(node_id);
        let mut second = service
            .runtime_membership(RequestContext::default(), second_requests)
            .await
            .unwrap()
            .body;

        assert!(matches!(
            second.next().await.unwrap().unwrap().directive,
            Some(controller_directive::Directive::Drain(_))
        ));
        assert_eq!(service.state.publish_desired_placements().await.unwrap(), 0);
        assert_eq!(
            service
                .state
                .allocate_and_publish_connection_budgets(node_id)
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn describe_node_drain_distinguishes_unknown_active_and_unmarkable_nodes() {
        let service = service();
        let never_seen = IncarnationId::new();
        assert_eq!(
            describe(&service, never_seen).await,
            (false, pb::NodeDrainState::NODE_DRAIN_STATE_UNSPECIFIED)
        );
        assert!(
            !service
                .mark_node_draining(RequestContext::default(), mark_request(never_seen))
                .await
                .unwrap()
                .body
                .accepted
        );

        let active = IncarnationId::new();
        let (_tx, requests) = open_requests(active);
        let _directives = service
            .runtime_membership(RequestContext::default(), requests)
            .await
            .unwrap()
            .body;
        assert_eq!(
            describe(&service, active).await,
            (true, pb::NodeDrainState::NODE_DRAIN_STATE_ACTIVE)
        );
    }
}
