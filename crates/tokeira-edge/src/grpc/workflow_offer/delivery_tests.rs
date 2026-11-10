//! In-process production handler and sink preparation checks; no listener binds.

use super::*;
use crate::{
    EdgeInterceptors, InMemoryNamespaceCache, WorkflowService,
    grpc::{runtime_adapter::RuntimeAdapter, workflow_dispatch::WorkflowTaskDispatchGrpc},
};
use proptest::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokeira_auth::peer::{PeerMaterial, PeerVerifier};
use tokeira_runtime::{
    LaneConfig, TimerScannerConfig, TokeiraRuntime, WorkflowTimeoutScannerConfig,
};
use tokeira_storage::InMemoryStore;
use tokeira_types::{
    BundleOwner, IncarnationId, NodeEndpoint, PlacementConfig, RoutingSnapshot, ShardEpoch,
};
use wire::workflow_task_dispatch_client::WorkflowTaskDispatchClient;

fn peer() -> Arc<PeerOfferEndpoint> {
    let material = PeerMaterial::generate().unwrap();
    Arc::new(PeerOfferEndpoint::new(
        PeerVerifier::new(material.profile().clone()).unwrap(),
        material.signer(material.profile()).unwrap(),
    ))
}
fn offer(index: u32) -> WorkflowOffer {
    decode(&wire::OfferWorkflowTaskRequest {
        run_key: Uuid::from_u128(u128::from(index) + 1).as_bytes().to_vec(),
        logical_seq: 1,
        namespace_id: Uuid::from_u128(17).as_bytes().to_vec(),
        selected_queue: Some(wire::OfferQueue {
            name: format!("queue-{index}"),
            ..Default::default()
        }),
        queue_kind: wire::OfferQueueKind::Normal as i32,
        routing: Some(wire::OfferRouting {
            mode: wire::OfferRoutingMode::Live as i32,
            ..Default::default()
        }),
        version_directive: Some(wire::OfferVersionDirective {
            behavior: wire::OfferVersionBehavior::Unversioned as i32,
            ..Default::default()
        }),
        scheduled_at: Some(prost_types::Timestamp {
            seconds: 1000,
            nanos: 17,
        }),
        workflow_id: format!("workflow-{index}"),
        ..Default::default()
    })
    .unwrap()
}
fn routing() -> Arc<RoutingCache> {
    let placement = PlacementConfig {
        shard_count: 8,
        bundle_count: 8,
        partition_count: 8,
        hash_version: 1,
    };
    let cache = Arc::new(RoutingCache::new(placement));
    let node = IncarnationId(Uuid::from_u128(9));
    let mut snapshot = RoutingSnapshot::new(placement);
    for id in 0..8 {
        snapshot.execution_bundle_owners.insert(
            ShardId(id),
            BundleOwner {
                node_id: node,
                epoch: ShardEpoch::ZERO,
            },
        );
    }
    snapshot.node_endpoints.insert(
        node,
        NodeEndpoint {
            host: "trusted-peer.invalid".into(),
            port: 7233,
        },
    );
    cache.replace_snapshot(snapshot);
    cache
}
fn service(
    runtime: Arc<TokeiraRuntime<InMemoryStore>>,
    peer: Arc<PeerOfferEndpoint>,
) -> WorkflowTaskDispatchGrpc<InMemoryStore> {
    let namespaces = Arc::new(InMemoryNamespaceCache::new());
    let interceptors = Arc::new(EdgeInterceptors::permissive(namespaces.clone()));
    let workflow = WorkflowService::new_with_buffered_queries_and_history_wait_registry(
        Arc::new(RuntimeAdapter::new(runtime.clone())),
        Arc::new(crate::InMemoryExecutionResolver::new()),
        Arc::new(crate::EmptyVisibilityApi),
        runtime.repo().clone(),
        Arc::new(crate::InMemoryOperatorApi::new("test", "test")),
        namespaces,
        interceptors.clone(),
        crate::PollerRegistry::default(),
        crate::PendingQueryStore::default(),
        runtime.buffered_queries(),
        runtime.broker(),
        crate::LongPollGate::new(crate::LongPollConfig::default()),
        Arc::new(crate::LocalOnlyRouter),
        crate::HistoryWaitRegistry::default(),
    );
    WorkflowTaskDispatchGrpc::new(runtime, interceptors, workflow).with_peer(Some(peer))
}
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: queue-home-delivery, Property 7: the real gated handler acknowledges retained hints without any start or run read.
    #[test]
    fn property_7_handler_and_sink_in_process(index in 0u32..1000, repeats in 1usize..8, bad_auth in any::<bool>()) {
        runtime().block_on(async {
            let repo = Arc::new(InMemoryStore::default());
            let node = Arc::new(TokeiraRuntime::new(repo.clone(), 1, LaneConfig::default(), TimerScannerConfig::default(), WorkflowTimeoutScannerConfig::default()));
            let peer = peer(); let cache = routing();
            let sink = PeerWorkflowOfferSink::new(cache, "unused-controller".into(), peer.clone());
            let mut client = WorkflowTaskDispatchClient::new(super::super::OfferResponseLimit::new(service(node.clone(), peer.clone()).into_service()));
            let offer = offer(index);
            prop_assert_eq!(decode(&encode(&offer, ShardId(0))).unwrap(), offer.clone());
            let before = repo.snapshot().await.unwrap();
            for copy in 0..repeats {
                let prepared = sink.prepare(&offer, ShardId(0)).unwrap();
                let mut request = prepared.request;
                if bad_auth { request.metadata_mut().remove("authorization"); }
                let result = client.offer_workflow_task(request).await;
                if bad_auth { prop_assert_eq!(result.unwrap_err().code(), tonic::Code::Unauthenticated); }
                else { prop_assert_eq!(outcome(result.unwrap().get_ref()).unwrap(), if copy == 0 { WorkflowOfferOutcome::Admitted } else { WorkflowOfferOutcome::Known }); }
            }
            prop_assert_eq!(repo.snapshot().await.unwrap(), before);
            node.shutdown_handle().begin_shutdown();
            Ok(())
        })?;
    }

    // Feature: queue-home-delivery, Property 7: colocated homes share a peer cap and every attempt resolves the trusted snapshot.
    #[test]
    fn property_7_sink_placement_and_destination_cap(index in 0u32..1000, released in 1usize..9) {
        runtime().block_on(async {
            let cache = routing(); let connects = Arc::new(AtomicUsize::new(0)); let observed = connects.clone();
            let sink = PeerWorkflowOfferSink::new(cache.clone(), "unused-controller".into(), peer()).with_peer_connector(Arc::new(move |endpoint| {
                assert_eq!(endpoint.uri().host(), Some("trusted-peer.invalid")); observed.fetch_add(1, Ordering::SeqCst); endpoint.connect_lazy()
            }));
            let offer = offer(index);
            let mut active = Vec::new();
            for home in 0..8 { active.push(sink.prepare(&offer, ShardId(home)).unwrap()); }
            prop_assert!(matches!(sink.prepare(&offer, ShardId(0)), Err(WorkflowOfferError::Capacity)));
            prop_assert_eq!(connects.load(Ordering::SeqCst), 1);
            for _ in 0..released { active.pop(); }
            for home in 0..released { active.push(sink.prepare(&offer, ShardId(home as u32)).unwrap()); }
            prop_assert!(matches!(sink.prepare(&offer, ShardId(0)), Err(WorkflowOfferError::Capacity)));
            active.clear(); prop_assert!(sink.destinations.lock().unwrap().is_empty());
            let mut snapshot = (*cache.snapshot()).clone(); snapshot.execution_bundle_owners.remove(&ShardId(0)); cache.replace_snapshot(snapshot);
            prop_assert!(matches!(sink.prepare(&offer, ShardId(0)), Err(WorkflowOfferError::Routing)));
            for reason in [3, 4] { prop_assert_eq!(outcome(&wire::OfferWorkflowTaskResponse { outcome: wire::OfferOutcome::StickyUnavailable as i32, sticky_unavailable_reason: Some(reason) }), Err(WorkflowOfferError::Invalid)); }
            Ok(())
        })?;
    }

    // Feature: queue-home-delivery, Property 4: the wire keeps every routing operand and both origins through normal fallback.
    #[test]
    fn property_4_wire_routing_and_sticky_origin(index in 0u32..1000, ramping in any::<bool>(), fallback in any::<bool>(), assigned in any::<bool>()) {
        let mut offer = offer(index);
        let normal = offer.hint.task.queue.clone();
        let sticky = QueueKey { task_queue: TaskQueueName("sticky".into()), ..normal.clone() };
        offer.hint.sticky_origin = Some(StickyWorkflowOfferOrigin { sticky_queue: sticky.clone(), normal_queue: normal.clone(), worker: WorkerIdentity("worker".into()), deadline: offer.hint.scheduled_at + time::Duration::seconds(30) });
        offer.hint.task.sticky_deadline = offer.hint.sticky_origin.as_ref().map(|o| o.deadline);
        if !fallback { offer.hint.task.queue = sticky; offer.hint.task.sticky_preferred = Some(WorkerIdentity("worker".into())); offer.hint.task.normal_queue = Some(normal); }
        offer.directive.use_ramping_version = ramping;
        offer.directive.assigned_build_id = assigned.then(|| BuildId("assigned".into()));
        offer.directive.behavior = VersioningBehavior::AutoUpgrade;
        offer.directive.deployment = Some(WorkerDeploymentVersionRef { deployment_name: "deploy".into(), build_id: "version".into() });
        let request = encode(&offer, ShardId(3));
        prop_assert_eq!(decode(&request).unwrap(), offer);
        let mut invalid = request; invalid.workflow_id.clear(); prop_assert!(decode(&invalid).is_err());
    }
}
