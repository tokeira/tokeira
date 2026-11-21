//! Authenticated generated-client checks against the production service, without sockets.

use std::sync::Arc;

use proptest::prelude::*;
use tokeira_auth::peer::{PeerMaterial, PeerVerifier};
use tokeira_runtime::{LaneConfig, TokeiraRuntime};
use tokeira_storage::InMemoryStore;

use super::{
    super::workflow_offer::{OfferGate, OfferResponseLimit, PeerOfferEndpoint},
    ActivityTaskDispatchGrpc,
    offer::{decode, encode, outcome},
};
use crate::{EdgeInterceptors, InMemoryNamespaceCache};
use time::OffsetDateTime;
use tokeira_proto::internal::runtime::{
    self as wire, activity_task_dispatch_client::ActivityTaskDispatchClient,
};
use tokeira_runtime::{ActivityOfferError, ActivityOfferOutcome};
use tokeira_types::ShardId;
use tonic::Code;
use tower::Service;
use uuid::Uuid;

#[derive(Debug)]
struct Clock;
impl tokeira_clock::Clock for Clock {
    fn now(&self) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(10_000).unwrap()
    }
}
fn node() -> Arc<TokeiraRuntime<InMemoryStore>> {
    Arc::new(TokeiraRuntime::new_with_nexus_and_shards_and_endpoint(
        Arc::new(InMemoryStore::default()),
        1,
        LaneConfig::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Arc::new(tokeira_runtime::NoopNexusHttpClient),
        Default::default(),
        1,
        "local".into(),
        "local".into(),
        true,
        None,
        Arc::new(Clock),
    ))
}
fn peer() -> Arc<PeerOfferEndpoint> {
    let material = PeerMaterial::generate().unwrap();
    Arc::new(
        PeerOfferEndpoint::new(
            PeerVerifier::new(material.profile().clone()).unwrap(),
            material.signer(material.profile()).unwrap(),
        )
        .with_clock(Arc::new(Clock)),
    )
}
fn service(
    node: Arc<TokeiraRuntime<InMemoryStore>>,
    peer: Option<Arc<PeerOfferEndpoint>>,
) -> ActivityTaskDispatchGrpc<InMemoryStore> {
    ActivityTaskDispatchGrpc::new(
        node,
        Arc::new(EdgeInterceptors::permissive(Arc::new(
            InMemoryNamespaceCache::new(),
        ))),
        peer,
    )
}
fn body(index: u128, stamp: u64, revision: i64) -> wire::OfferActivityTaskRequest {
    wire::OfferActivityTaskRequest {
        run_key: Uuid::from_u128(index + 1).as_bytes().to_vec(),
        activity_id: "activity".into(),
        schedule_event_id: 1,
        attempt: u32::MAX,
        stamp,
        execution_home: 0,
        destination_home: 0,
        namespace_id: Uuid::from_u128(3).as_bytes().to_vec(),
        selected_queue: Some(wire::OfferQueue {
            name: "queue".into(),
            deployment: Some(String::new()),
            build_id: Some(String::new()),
        }),
        dispatch_revision: revision,
        dispatch_at: Some(prost_types::Timestamp {
            seconds: 10_000,
            nanos: 0,
        }),
        priority: Some(wire::OfferPriority {
            priority_key: i32::MIN,
            fairness_key: "fair".into(),
            fairness_weight: 0.0,
        }),
        expires_at: Some(prost_types::Timestamp {
            seconds: 10_005,
            nanos: 0,
        }),
    }
}
fn authorized(
    body: wire::OfferActivityTaskRequest,
    peer: &PeerOfferEndpoint,
) -> tonic::Request<wire::OfferActivityTaskRequest> {
    let mut request = tonic::Request::new(body);
    request.metadata_mut().insert(
        "authorization",
        peer.signer()
            .token_at(10_000)
            .unwrap()
            .authorization()
            .parse()
            .unwrap(),
    );
    request
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: activity-queue-home-delivery, Property 4: every field policy and peer purpose passes through pre-decode authorization and the real handler.
    #[test]
    fn activity_offer_authorized_wire_policy(index in 0u128..10000, stamp in any::<u64>(), revision in any::<i64>()) {
        tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap().block_on(async {
            let node = node(); let peer = peer();
            let mut client = ActivityTaskDispatchClient::new(OfferResponseLimit::new(service(node.clone(), Some(peer.clone())).into_service()));
            let valid = body(index, stamp, revision);
            let decoded = decode(&valid, 1).unwrap();
            prop_assert_eq!(encode(&decoded, ShardId(0)), valid.clone());
            for mutation in 0..16 {
                let mut bad = valid.clone();
                match mutation {
                    0 => bad.run_key.clear(), 1 => bad.run_key = vec![0; 16],
                    2 => bad.namespace_id.clear(), 3 => bad.namespace_id = vec![0; 16],
                    4 => bad.activity_id.clear(), 5 => bad.schedule_event_id = 0,
                    6 => bad.attempt = 0, 7 => bad.execution_home = 1, 8 => bad.destination_home = 1,
                    9 => bad.selected_queue = None, 10 => bad.selected_queue.as_mut().unwrap().name.clear(),
                    11 => bad.dispatch_at = None, 12 => bad.expires_at = None,
                    13 => bad.dispatch_at.as_mut().unwrap().nanos = -1,
                    14 => bad.expires_at = bad.dispatch_at,
                    _ => bad.priority.as_mut().unwrap().fairness_weight = f32::NAN,
                }
                prop_assert_eq!(client.offer_activity_task(authorized(bad, &peer)).await.unwrap_err().code(), Code::InvalidArgument);
            }
            for purpose in 0..4 {
                let mut request = tonic::Request::new(valid.clone());
                match purpose {
                    0 => {},
                    1 => { request.metadata_mut().insert("authorization", peer.signer().activity_start_token_at(10_000).unwrap().authorization().parse().unwrap()); },
                    2 => { request.metadata_mut().insert("authorization", peer.signer().outbound_token_at(10_000).unwrap().authorization().parse().unwrap()); },
                    _ => { request.metadata_mut().insert("authorization", "Bearer public-worker".parse().unwrap()); },
                }
                prop_assert_eq!(client.offer_activity_task(request).await.unwrap_err().code(), if purpose == 1 || purpose == 2 { Code::PermissionDenied } else { Code::Unauthenticated });
            }
            let mut oversized = authorized(valid.clone(), &peer);
            oversized.metadata_mut().insert("padding", "x".repeat(8192).parse().unwrap());
            prop_assert_eq!(client.offer_activity_task(oversized).await.unwrap_err().code(), Code::ResourceExhausted);
            let mut oversized = valid.clone(); oversized.activity_id = "x".repeat(65536);
            prop_assert_eq!(client.offer_activity_task(authorized(oversized, &peer)).await.unwrap_err().code(), Code::ResourceExhausted);
            let mut future = valid.clone(); future.dispatch_at.as_mut().unwrap().seconds += 1;
            prop_assert_eq!(outcome(client.offer_activity_task(authorized(future, &peer)).await.unwrap().get_ref()).unwrap(), ActivityOfferOutcome::NotDue);
            let mut expired = valid.clone(); expired.dispatch_at.as_mut().unwrap().seconds -= 2; expired.expires_at.as_mut().unwrap().seconds = 9999;
            prop_assert_eq!(outcome(client.offer_activity_task(authorized(expired, &peer)).await.unwrap().get_ref()).unwrap(), ActivityOfferOutcome::Expired);
            for expected in [ActivityOfferOutcome::Admitted, ActivityOfferOutcome::Known] {
                prop_assert_eq!(outcome(client.offer_activity_task(authorized(valid.clone(), &peer)).await.unwrap().get_ref()).unwrap(), expected);
            }
            let mut gate = OfferGate::new(tower::service_fn(|_: http::Request<tonic::body::Body>| std::future::pending::<Result<http::Response<tonic::body::Body>, std::convert::Infallible>>()), Some(peer.clone()));
            let raw = || http::Request::builder().uri(super::offer::OFFER_PATH)
                .header("authorization", peer.signer().token_at(10_000).unwrap().authorization())
                .body(tonic::body::Body::empty()).unwrap();
            let mut held: Vec<_> = (0..64).map(|_| gate.call(raw())).collect();
            let exhausted = gate.call(raw()).await.unwrap();
            prop_assert_eq!(&exhausted.headers()["grpc-status"], "8");
            drop(held.pop());
            let mut replacement = gate.call(raw());
            prop_assert!(std::future::poll_fn(|cx| std::task::Poll::Ready(replacement.as_mut().poll(cx).is_pending())).await);
            drop(replacement); drop(held);
            for invalid in [0, -1, 7, i32::MAX] { prop_assert_eq!(outcome(&wire::OfferActivityTaskResponse { outcome: invalid }), Err(ActivityOfferError::Invalid)); }
            let mut absent = ActivityTaskDispatchClient::new(service(node.clone(), None).into_service());
            prop_assert_eq!(absent.offer_activity_task(tonic::Request::new(valid)).await.unwrap_err().code(), Code::Unimplemented);
            node.shutdown_handle().begin_shutdown();
            Ok(())
        })?;
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: activity-queue-home-delivery, Property 4: an authenticated duplicate cannot acknowledge a retired receiver; replacement admission uses its own acquisition.
    #[test]
    fn activity_offer_authorized_owner_movement(index in 0u128..10000, stamp in any::<u64>(), replacement in any::<bool>()) {
        tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap().block_on(async {
            let node = node(); let peer = peer();
            let mut client = ActivityTaskDispatchClient::new(OfferResponseLimit::new(service(node.clone(), Some(peer.clone())).into_service()));
            let valid = body(index, stamp, 0);
            prop_assert_eq!(outcome(client.offer_activity_task(authorized(valid.clone(), &peer)).await.unwrap().get_ref()).unwrap(), ActivityOfferOutcome::Admitted);
            let delayed = authorized(valid.clone(), &peer);
            node.relinquish_shard(ShardId(0)).await;
            prop_assert_eq!(outcome(client.offer_activity_task(delayed).await.unwrap().get_ref()).unwrap(), ActivityOfferOutcome::NotHome);
            if replacement {
                node.recover_self_assigned_shard(ShardId(0), tokeira_types::ShardEpoch::ZERO).await.unwrap();
                prop_assert_eq!(outcome(client.offer_activity_task(authorized(valid.clone(), &peer)).await.unwrap().get_ref()).unwrap(), ActivityOfferOutcome::Admitted);
                prop_assert_eq!(outcome(client.offer_activity_task(authorized(valid, &peer)).await.unwrap().get_ref()).unwrap(), ActivityOfferOutcome::Known);
            }
            let shutdown = node.shutdown_handle();
            shutdown.begin_shutdown();
            shutdown.wait(std::time::Instant::now() + std::time::Duration::from_secs(1)).await.unwrap();
            Ok(())
        })?;
    }
}
