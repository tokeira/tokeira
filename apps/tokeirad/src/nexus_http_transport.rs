//! Same-listener transport adapter for caller-facing Nexus HTTP requests.
//!
//! Tonic owns the public socket and accepts HTTP/1 for gRPC-Web. This tower
//! layer intercepts only the two Nexus route prefixes, bounds and collects their
//! bodies, and delegates all other requests to tonic unchanged. Protocol and
//! authorization decisions remain in `tokeira-edge::nexus_http`.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use bytes::Bytes;
use http_body_legacy::Body as _;
use hyper_legacy::{Body, Request, Response};
use tokeira_edge::nexus_http::{
    MAX_NEXUS_PAYLOAD_BYTES, NexusHttpHandler, NexusHttpRequest, NexusHttpResponse,
};
use tonic::body::BoxBody;
use tower::{Layer, Service};

/// Tower layer routing caller-facing Nexus HTTP requests to the edge handler.
#[derive(Clone, Debug)]
pub struct NexusHttpLayer {
    handler: Arc<NexusHttpHandler>,
}

impl NexusHttpLayer {
    /// Construct the layer around a shared edge handler.
    pub fn new(handler: Arc<NexusHttpHandler>) -> Self {
        Self { handler }
    }
}

impl<S> Layer<S> for NexusHttpLayer {
    type Service = NexusHttpService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        NexusHttpService {
            inner,
            handler: self.handler.clone(),
        }
    }
}

/// Service produced by [`NexusHttpLayer`].
#[derive(Clone, Debug)]
pub struct NexusHttpService<S> {
    inner: S,
    handler: Arc<NexusHttpHandler>,
}

impl<S> Service<Request<Body>> for NexusHttpService<S>
where
    S: Service<Request<Body>, Response = Response<BoxBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
{
    type Response = Response<BoxBody>;
    type Error = S::Error;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        if !NexusHttpHandler::recognizes_path(request.uri().path()) {
            return Box::pin(self.inner.call(request));
        }

        let handler = self.handler.clone();
        Box::pin(async move {
            let (parts, mut body) = request.into_parts();
            let mut collected = Vec::new();
            let mut body_too_large = false;
            let mut body_read_failed = false;
            while let Some(chunk) = body.data().await {
                match chunk {
                    Ok(chunk) => {
                        if collected.len().saturating_add(chunk.len()) > MAX_NEXUS_PAYLOAD_BYTES {
                            body_too_large = true;
                            break;
                        }
                        collected.extend_from_slice(&chunk);
                    }
                    Err(_) => {
                        body_read_failed = true;
                        break;
                    }
                }
            }
            let headers = parts
                .headers
                .iter()
                .filter_map(|(name, value)| {
                    value
                        .to_str()
                        .ok()
                        .map(|value| (name.as_str().to_owned(), value.to_owned()))
                })
                .collect();
            let response = handler
                .handle(NexusHttpRequest {
                    method: parts.method.as_str().to_owned(),
                    path: parts.uri.path().to_owned(),
                    query: parts.uri.query().map(str::to_owned),
                    headers,
                    body: collected,
                    body_too_large,
                    body_read_failed,
                })
                .await;
            Ok(to_http_response(response))
        })
    }
}

fn to_http_response(response: NexusHttpResponse) -> Response<BoxBody> {
    let mut builder = Response::builder().status(response.status);
    for (name, value) in response.headers {
        builder = builder.header(name, value);
    }
    let body = http_body_legacy::Full::new(Bytes::from(response.body))
        .map_err(|never| match never {})
        .boxed_unsync();
    builder.body(body).unwrap_or_else(|_| {
        Response::builder()
            .status(500)
            .body(tonic::body::empty_body())
            .expect("static fallback response is valid")
    })
}

#[cfg(test)]
mod tests {
    use std::{
        convert::Infallible,
        future::{Ready, ready},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use tokeira_edge::{
        namespace_cache::{InMemoryNamespaceCache, NamespaceCache, ResolvedNamespace},
        nexus_http::{NexusHttpHandler, NexusHttpWaiterRegistry, PermissiveNexusHttpAuthorizer},
    };
    use tokeira_runtime::{NexusTaskBroker, nexus::InMemoryNexusEndpointStore};

    use super::*;

    #[derive(Clone, Debug)]
    struct CountingService {
        calls: Arc<AtomicUsize>,
    }

    impl Service<Request<Body>> for CountingService {
        type Response = Response<BoxBody>;
        type Error = Infallible;
        type Future = Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _request: Request<Body>) -> Self::Future {
            self.calls.fetch_add(1, Ordering::SeqCst);
            ready(Ok(Response::builder()
                .status(204)
                .body(tonic::body::empty_body())
                .expect("static response")))
        }
    }

    async fn test_handler() -> Arc<NexusHttpHandler> {
        let namespaces = Arc::new(InMemoryNamespaceCache::new());
        namespaces
            .insert(ResolvedNamespace::active("default"))
            .await
            .expect("namespace");
        Arc::new(NexusHttpHandler::new(
            namespaces,
            Arc::new(InMemoryNexusEndpointStore::new()),
            NexusTaskBroker::default(),
            NexusHttpWaiterRegistry::default(),
            Arc::new(PermissiveNexusHttpAuthorizer),
        ))
    }

    #[tokio::test]
    async fn unrelated_requests_are_delegated_to_the_inner_service() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut service = NexusHttpLayer::new(test_handler().await).layer(CountingService {
            calls: calls.clone(),
        });
        let request = Request::builder()
            .method("POST")
            .uri("/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo")
            .body(Body::empty())
            .expect("request");

        let response = service.call(request).await.expect("infallible");
        assert_eq!(response.status(), 204);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn nexus_requests_are_intercepted_and_bounded_on_the_shared_listener() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut service = NexusHttpLayer::new(test_handler().await).layer(CountingService {
            calls: calls.clone(),
        });
        let request = Request::builder()
            .method("POST")
            .uri("/namespaces/default/task-queues/queue/nexus-services/service/operation")
            .header("content-type", "application/json")
            .body(Body::from(vec![b'a'; MAX_NEXUS_PAYLOAD_BYTES + 1]))
            .expect("request");

        let response = service.call(request).await.expect("infallible");
        assert_eq!(response.status(), 400);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}
