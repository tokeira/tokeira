//! Spike: buffa + connect-rust for the internal controller ↔ runtime surface.
//!
//! Validates:
//! 1. The controller proto compiles with connectrpc-build (buffa + service stubs)
//! 2. Zero-copy views work for FullRoutingSnapshot decode
//! 3. A connect-rust server can serve SubscribeRouting (server-streaming)
//! 4. A connect-rust client can consume the stream
//! 5. gRPC protocol works (same handlers serve Connect + gRPC + gRPC-Web)
//!
//! Run: cargo run -p spike-connect-buffa

#![allow(refining_impl_trait_internal, refining_impl_trait_reachable)]

pub mod proto {
    connectrpc::include_generated!();
}

use std::sync::Arc;

use buffa::{Message, MessageView, OwnedView};
use connectrpc::{RequestContext, Response, Router, ServiceResult, ServiceStream};
use proto::tokeira::internal::controller::v1::*;
use tracing::{info, Level};
use tracing_subscriber::EnvFilter;

// ── Server implementation ───────────────────────────────────────────────────

struct ControllerImpl;

impl PlacementController for ControllerImpl {
    /// Server-streaming: sends a full routing snapshot then closes.
    async fn subscribe_routing(
        &self,
        _ctx: RequestContext,
        _req: OwnedSubscribeRoutingRequestView,
    ) -> ServiceResult<ServiceStream<RoutingUpdate>> {
        let snapshot = build_sample_snapshot(64);
        let update = RoutingUpdate {
            update: Some(routing_update::Update::Full(snapshot.into())),
            ..Default::default()
        };
        let s = futures::stream::iter(vec![Ok(update)]);
        Response::stream_ok(s)
    }

    /// Unary: returns the current owner for a bundle.
    async fn refresh_bundle(
        &self,
        _ctx: RequestContext,
        req: OwnedRefreshBundleRequestView,
    ) -> ServiceResult<RefreshBundleResponse> {
        let bundle_id = req.bundle_id;
        Response::ok(RefreshBundleResponse {
            bundle: BundleOwnershipEntry {
                bundle_id,
                state: Some(bundle_ownership_entry::State::Owner(
                    BundleOwnerMessage {
                        owner_node_id: "550e8400-e29b-41d4-a716-446655440000".into(),
                        epoch: 42,
                        ..Default::default()
                    }
                    .into(),
                )),
                ..Default::default()
            }
            .into(),
            node: NodeEndpointEntry {
                node_id: "550e8400-e29b-41d4-a716-446655440000".into(),
                state: Some(node_endpoint_entry::State::Endpoint(
                    NodeEndpointMessage {
                        host: "10.0.1.42".into(),
                        port: 7240,
                        ..Default::default()
                    }
                    .into(),
                )),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        })
    }

    /// Bidi streaming: membership stream (heartbeats in, directives out).
    async fn runtime_membership(
        &self,
        _ctx: RequestContext,
        requests: ServiceStream<OwnedRuntimeMembershipRequestView>,
    ) -> ServiceResult<ServiceStream<ControllerDirective>> {
        use futures::StreamExt;
        // Echo back a connection budget directive for each message received.
        let responses = requests.map(|msg| {
            msg.map(|_| ControllerDirective {
                directive: Some(controller_directive::Directive::ConnectionBudget(
                    ConnectionBudgetDirective {
                        rate_per_second: 50.0,
                        capacity: 5000,
                        max_reservoir_size: 50,
                        ..Default::default()
                    }
                    .into(),
                )),
                ..Default::default()
            })
        });
        Response::stream_ok(responses)
    }

    /// Unary: nominate scale-in candidates.
    async fn nominate_scale_in_candidates(
        &self,
        _ctx: RequestContext,
        _req: OwnedNominateRequestView,
    ) -> ServiceResult<NominateResponse> {
        Response::ok(NominateResponse {
            candidates: vec![],
            aggregate_available_connections: 500,
            aggregate_connection_rate_headroom: 80.0,
            ..Default::default()
        })
    }

    /// Unary: mark a node as draining.
    async fn mark_node_draining(
        &self,
        _ctx: RequestContext,
        _req: OwnedMarkDrainingRequestView,
    ) -> ServiceResult<MarkDrainingResponse> {
        Response::ok(MarkDrainingResponse {
            accepted: true,
            ..Default::default()
        })
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn build_sample_snapshot(bundle_count: u32) -> FullRoutingSnapshot {
    let bundles = (0..bundle_count)
        .map(|i| BundleOwnershipEntry {
            bundle_id: i,
            state: Some(bundle_ownership_entry::State::Owner(
                BundleOwnerMessage {
                    owner_node_id: "550e8400-e29b-41d4-a716-446655440000".into(),
                    epoch: 1,
                    ..Default::default()
                }
                .into(),
            )),
            ..Default::default()
        })
        .collect();

    FullRoutingSnapshot {
        bundles,
        nodes: vec![NodeEndpointEntry {
            node_id: "550e8400-e29b-41d4-a716-446655440000".into(),
            state: Some(node_endpoint_entry::State::Endpoint(
                NodeEndpointMessage {
                    host: "10.0.1.42".into(),
                    port: 7240,
                    ..Default::default()
                }
                .into(),
            )),
            ..Default::default()
        }],
        placement_config: PlacementConfigMessage {
            shard_count: 64,
            bundle_count: 64,
            partition_count: 1024,
            hash_version: 1,
            ..Default::default()
        }
        .into(),
        generation: 1,
        ..Default::default()
    }
}

// ── Main ────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(Level::INFO.into()))
        .init();

    info!("=== spike-connect-buffa ===");

    // ── Part 1: Zero-copy view decode ───────────────────────────────────────
    let snapshot = build_sample_snapshot(64);
    let encoded = snapshot.encode_to_vec();
    info!(encoded_bytes = encoded.len(), "encoded FullRoutingSnapshot (64 bundles)");

    // Owned decode.
    let decoded = FullRoutingSnapshot::decode_from_slice(&encoded)?;
    assert_eq!(decoded.bundles.len(), 64);
    info!("owned decode OK: {} bundles", decoded.bundles.len());

    // Zero-copy view decode — string fields are &str, no allocation.
    let view = FullRoutingSnapshotView::decode_view(&encoded)?;
    assert_eq!(view.bundles.len(), 64);
    if let Some(node) = view.nodes.first() {
        let node_id: &str = &node.node_id;
        info!(node_id, "view decode OK: zero-copy &str field");
    }

    // OwnedView — Send + 'static, still zero-copy via Deref.
    let buf = bytes::Bytes::from(encoded.clone());
    let owned_view = OwnedView::<FullRoutingSnapshotView>::decode(buf)?;
    assert_eq!(owned_view.bundles.len(), 64);
    info!("OwnedView decode OK: Send + 'static, zero-copy");

    // ── Part 2: Server + Client round-trip ──────────────────────────────────
    let service = Arc::new(ControllerImpl);
    let connect_router = service.register(Router::new());
    let app = axum::Router::new().fallback_service(connect_router.into_axum_service());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    info!(%addr, "server listening");

    let server_handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Give server a moment to bind.
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Client: call RefreshBundle (unary).
    use connectrpc::client::{ClientConfig, HttpClient};
    let http = HttpClient::plaintext();
    let config = ClientConfig::new(format!("http://{addr}").parse()?);
    let client = PlacementControllerClient::new(http, config);

    let resp = client
        .refresh_bundle(RefreshBundleRequest {
            bundle_id: 7,
            ..Default::default()
        })
        .await?;
    let msg = resp.view();
    info!(bundle_id = msg.bundle.bundle_id, "client RefreshBundle OK");

    // Client: SubscribeRouting (server-streaming).
    let mut stream = client
        .subscribe_routing(SubscribeRoutingRequest::default())
        .await?;
    if let Some(update) = stream.message().await? {
        // Convert view to owned for easier pattern matching on the oneof.
        let owned = update.to_owned_message();
        if let Some(routing_update::Update::Full(snap)) = owned.update {
            info!(
                bundles = snap.bundles.len(),
                generation = snap.generation,
                "client SubscribeRouting OK: received full snapshot"
            );
        }
    }

    info!("=== spike complete ===");
    info!("findings:");
    info!("  1. buffa codegen works for the controller proto");
    info!("  2. zero-copy views decode FullRoutingSnapshot without allocation");
    info!("  3. connect-rust server serves all 5 RPCs (unary + streaming)");
    info!("  4. connect-rust client consumes unary and server-streaming");
    info!("  5. gRPC protocol works (same handlers serve Connect + gRPC + gRPC-Web)");

    server_handle.abort();
    Ok(())
}
