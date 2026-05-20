//! Connect-rust service implementation for the placement controller.
//!
//! Wraps `PlacementControllerState` and implements the connect-rust
//! `PlacementController` trait. Provides zero-copy request views and
//! serves Connect, gRPC, and gRPC-Web on the same handlers.

use connectrpc::{RequestContext, Response, ServiceResult, ServiceStream};
use futures::StreamExt;
use tokeira_proto::connect::tokeira::internal::controller::v1::{
    self as pb, BundleOwnerMessage, BundleOwnershipEntry,
    ControllerDirective, FullRoutingSnapshot, MarkDrainingResponse, NodeEndpointEntry,
    NodeEndpointMessage, NominateResponse, PlacementConfigMessage, PlacementController,
    RefreshBundleResponse, RoutingUpdate, ScaleInCandidate,
    bundle_ownership_entry, node_endpoint_entry, routing_update,
    OwnedMarkDrainingRequestView, OwnedNominateRequestView, OwnedRefreshBundleRequestView,
    OwnedRuntimeMembershipRequestView, OwnedSubscribeRoutingRequestView,
};
use tokeira_types::{IncarnationId, PlacementConfig, ShardId};

use crate::{
    PlacementControllerState,
    membership::{NodeDrainState, RuntimeHeartbeat, RuntimeRegistration},
};

/// Connect-rust service wrapper around the shared controller state.
pub struct ConnectPlacementController {
    pub state: PlacementControllerState,
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
    ) -> ServiceResult<ServiceStream<ControllerDirective>> {
        let state = self.state.clone();
        let (tx, rx) = tokio::sync::mpsc::channel::<ControllerDirective>(16);

        tokio::spawn(async move {
            use pb::runtime_membership_request::Request;

            let mut stream = requests;
            let mut node_id: Option<IncarnationId> = None;

            while let Some(msg) = stream.next().await {
                let Ok(msg) = msg else { break };
                let owned = msg.to_owned_message();

                match owned.request {
                    Some(Request::Registration(reg)) => {
                        let incarnation = match reg.node_id.parse::<uuid::Uuid>() {
                            Ok(uuid) => IncarnationId(uuid),
                            Err(_) => break,
                        };
                        node_id = Some(incarnation);
                        let registration = RuntimeRegistration {
                            node_id: incarnation,
                            host: reg.host,
                            port: reg.port as u16,
                            zone: if reg.zone.is_empty() {
                                None
                            } else {
                                Some(reg.zone)
                            },
                            version: reg.version,
                            build_id: reg.build_id,
                        };
                        state
                            .membership
                            .write()
                            .await
                            .register_node(registration, RuntimeHeartbeat::empty(), None);
                    }
                    Some(Request::Heartbeat(hb)) => {
                        if let Some(nid) = node_id {
                            let heartbeat = decode_heartbeat(&hb);
                            state
                                .drain
                                .write()
                                .await
                                .record_progress(nid, heartbeat.drain_state);
                            state
                                .membership
                                .write()
                                .await
                                .update_heartbeat(nid, heartbeat);
                        }
                    }
                    None => {}
                }
            }
            drop(tx);
        });

        let response_stream = tokio_stream::wrappers::ReceiverStream::new(rx).map(Ok);
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
        let node_id = match req.node_id.parse::<uuid::Uuid>() {
            Ok(uuid) => IncarnationId(uuid),
            Err(_) => {
                return Err(connectrpc::ConnectError::new(
                    connectrpc::ErrorCode::InvalidArgument,
                    "invalid node_id UUID",
                ));
            }
        };

        let accepted = self.state.membership.write().await.mark_draining(node_id);
        if accepted {
            self.state.drain.write().await.mark_draining(node_id);
        }

        Response::ok(MarkDrainingResponse {
            accepted,
            ..Default::default()
        })
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

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
                    epoch: owner.epoch.0 as u64,
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
                    epoch: lease.epoch.0 as u64,
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
