//! A trusted placement endpoint for the sender's real refresh client.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use http_body_util::{BodyExt, Full};
use hyper::{Response, service::service_fn};
use hyper_util::rt::{TokioExecutor, TokioIo};
use prost::Message;
use tokeira_proto::internal::controller::{
    BundleOwnerMessage, BundleOwnershipEntry, NodeEndpointEntry, NodeEndpointMessage,
    RefreshBundleRequest, RefreshBundleResponse, bundle_ownership_entry, node_endpoint_entry,
};
use tokeira_types::{ShardEpoch, ShardId};
use tokio_util::sync::CancellationToken;

pub(super) struct Controller {
    pub(super) endpoint: String,
    pub(super) calls: Arc<AtomicUsize>,
    response: Arc<Mutex<RefreshBundleResponse>>,
    unexpected: Arc<Mutex<Vec<RefreshBundleRequest>>>,
    cancel: CancellationToken,
    server: tokio::task::JoinHandle<()>,
}
impl Controller {
    pub(super) async fn new() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("placement refresh test requires local sockets");
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let response = Arc::new(Mutex::new(RefreshBundleResponse::default()));
        let unexpected = Arc::new(Mutex::new(Vec::new()));
        let unexpected_requests = unexpected.clone();
        let cancel = CancellationToken::new();
        let shutdown = cancel.clone();
        let observed = calls.clone();
        let selected = response.clone();
        let server = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    accepted = listener.accept() => {
                        let (stream, _) = accepted.expect("accept controller request");
                        let calls = observed.clone();
                        let response = selected.clone();
                        let unexpected = unexpected_requests.clone();
                        connections.spawn(async move {
                            let service = service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
                                let calls = calls.clone();
                                let response = response.clone();
                                let unexpected = unexpected.clone();
                                async move {
                                    assert!(request.uri().path().ends_with("/RefreshBundle"), "controller request path: {}", request.uri().path());
                                    let body = request.into_body().collect().await.expect("controller body").to_bytes();
                                    let request = RefreshBundleRequest::decode(body).expect("decode real refresh request");
                                    let response = response.lock().unwrap().clone();
                                    calls.fetch_add(1, Ordering::SeqCst);
                                    if Some(request.bundle_id) != response.bundle.as_ref().map(|b| b.bundle_id) {
                                        unexpected.lock().unwrap().push(request);
                                        return Ok::<_, std::convert::Infallible>(Response::builder().status(hyper::StatusCode::SERVICE_UNAVAILABLE).body(Full::new(bytes::Bytes::new())).unwrap());
                                    }
                                    Ok::<_, std::convert::Infallible>(Response::builder().header("content-type", "application/proto").body(Full::new(bytes::Bytes::from(response.encode_to_vec()))).unwrap())
                                }
                            });
                            hyper_util::server::conn::auto::Builder::new(TokioExecutor::new()).serve_connection(TokioIo::new(stream), service).await.expect("serve controller response");
                        });
                    }
                }
            }
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        });
        Self {
            endpoint,
            calls,
            response,
            unexpected,
            cancel,
            server,
        }
    }

    pub(super) fn point_to(
        &self,
        home: ShardId,
        index: usize,
        epoch: ShardEpoch,
        address: std::net::SocketAddr,
    ) {
        let id = uuid::Uuid::from_u128(index as u128 + 1).to_string();
        *self.response.lock().unwrap() = RefreshBundleResponse {
            bundle: Some(BundleOwnershipEntry {
                bundle_id: home.0,
                state: Some(bundle_ownership_entry::State::Owner(BundleOwnerMessage {
                    owner_node_id: id.clone(),
                    epoch: epoch.0,
                })),
            }),
            node: Some(NodeEndpointEntry {
                node_id: id,
                state: Some(node_endpoint_entry::State::Endpoint(NodeEndpointMessage {
                    host: address.ip().to_string(),
                    port: address.port().into(),
                })),
            }),
        };
    }

    pub(super) fn assert_no_unexpected_requests(&self) {
        let requests = self.unexpected.lock().unwrap();
        assert!(
            requests.is_empty(),
            "unexpected placement refresh requests: {requests:?}"
        );
    }

    pub(super) async fn stop(mut self) {
        self.cancel.cancel();
        (&mut self.server).await.expect("controller stopped");
        self.assert_no_unexpected_requests();
    }
}
