//! Wire-coverage tower layer for Tier-2 functional conformance.
//!
//! Tier 2 runs Temporal's own functional Go suite, unmodified, over the real gRPC wire
//! against a running `tokeirad` (see `.kiro/specs/temporal-functional-conformance`). To
//! turn a run into an interpretable coverage report, every served
//! `(wire_method, status_code)` pair must be captured *faithfully* — as the true wire
//! path (`/package.Service/Method`) and the real gRPC status, exactly what
//! `tokeira_compatibility::coverage::resolve` consumes. This module owns that capture
//! point: a tower [`Layer`] that wraps the gRPC transport boundary and feeds every call
//! into the shared [`WireCoverageRecorder`].
//!
//! ## Why the transport boundary, not the admission seam
//!
//! Capture lives here — at the tonic `Server`'s tower stack — rather than in
//! `EdgeInterceptors`, because the admission seam carries a snake_case internal `Action`,
//! not the wire path. Reconstructing `/package.Service/Method` from an `Action` would be
//! a lossy, drift-prone mapping that the report would then have to trust. At the
//! transport boundary the wire path is already present verbatim on `req.uri().path()`,
//! and the gRPC status is on the response — so the recorder observes precisely what the
//! suite drove and precisely what `resolve` keys on, with no reconstruction. This is the
//! design's "faithful capture point" (R5).
//!
//! ## Why zero overhead when off
//!
//! The layer is mounted on the gRPC server *only* when a conformance run sets the flag
//! (`tokeirad` checks the env and constructs the recorder under it). Production never
//! installs the layer, so there is no per-call cost, no shared counter, and no recorder
//! at all on the hot path. The recorder's own never-production invariant (see
//! [`super::recorder`]) is upheld by this gating: the only thing that ever calls
//! `recorder.record` is this layer, and the layer is only ever in the stack under the
//! flag.
//!
//! ## Why the response body is never touched
//!
//! The layer reads only the response *headers* for `grpc-status` and passes the
//! `http::Response` through unchanged. Buffering or polling the body would change
//! streaming semantics and add latency/memory to every call — unacceptable even for a
//! conformance build, and unnecessary: for the unary RPCs the public surface exposes,
//! tonic places `grpc-status` in the initial response headers, and a missing header on
//! an otherwise-successful response means OK (code 0) per the gRPC spec. Trailers-only
//! streaming statuses that are not in the header map are therefore conservatively read as
//! OK; this is acceptable because the report's highest-signal findings come from
//! explicit non-OK statuses, which tonic *does* surface in the header map for the unary
//! calls Tier 2 drives.
//!
//! ## Why the future is boxed
//!
//! [`WireCoverageService::call`] returns a `Pin<Box<dyn Future>>`. A hand-written future
//! would avoid one allocation per call, but this service is conformance-only and never on
//! a production hot path, so the allocation is irrelevant and the boxed future keeps the
//! type legible and the trait bounds tractable against tonic 0.11's `Server::layer`
//! requirements. Clarity is worth more than a saved box here.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use tonic::codegen::http::{Request, Response};
use tower::{Layer, Service};

use super::recorder::WireCoverageRecorder;

/// The gRPC trailer/header that carries the numeric status code of a call.
///
/// tonic writes the `tonic::Code as i32` here as ASCII decimal. The recorder stores the
/// same `i32` verbatim (see [`super::record::WireCoverageRow`]), so the layer parses this
/// header straight to `i32` with no enum round-trip.
const GRPC_STATUS_HEADER: &str = "grpc-status";

/// Read the gRPC status code from a response's header map.
///
/// Returns the parsed `tonic::Code as i32`, or `0` (OK) when the `grpc-status` header is
/// absent or unparsable. The OK-on-absent rule is the gRPC contract: a successful unary
/// response need not carry an explicit `grpc-status: 0`. The OK-on-unparsable rule is a
/// deliberate fidelity tradeoff — a malformed status header is corrupt *evidence*, never
/// a correctness concern for a path the layer only observes, so it degrades to OK rather
/// than failing the request or panicking. Kept as a free function so the parsing contract
/// is unit-testable without standing up a full tower `Service`.
fn grpc_status_code(headers: &tonic::codegen::http::HeaderMap) -> i32 {
    headers
        .get(GRPC_STATUS_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|text| text.trim().parse::<i32>().ok())
        .unwrap_or(0)
}

/// Tower [`Layer`] that records every served `(wire_method, status_code)` into a shared
/// [`WireCoverageRecorder`].
///
/// Constructed by `tokeirad` only under the conformance flag and mounted on the gRPC
/// `Server`'s tower stack; the `Arc<WireCoverageRecorder>` it holds is the same recorder a
/// later task snapshots and exports as JSON evidence. Cloning the layer (which tonic does
/// internally) clones the `Arc`, so all produced services feed one counter.
#[derive(Debug, Clone)]
pub struct WireCoverageLayer {
    /// The shared recorder every wrapped call increments. Held behind `Arc` so the layer,
    /// every `WireCoverageService` it produces, and the conformance exporter all observe
    /// the same counts.
    recorder: Arc<WireCoverageRecorder>,
}

impl WireCoverageLayer {
    /// Create a layer that feeds the given recorder.
    ///
    /// The caller (`tokeirad`, under the conformance flag) owns the recorder and keeps a
    /// clone so it can snapshot/export the observed coverage after the run; the layer only
    /// ever writes to it.
    pub fn new(recorder: Arc<WireCoverageRecorder>) -> Self {
        Self { recorder }
    }
}

impl<S> Layer<S> for WireCoverageLayer {
    type Service = WireCoverageService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        WireCoverageService {
            inner,
            recorder: Arc::clone(&self.recorder),
        }
    }
}

/// Tower [`Service`] produced by [`WireCoverageLayer`]; wraps an inner tonic service `S`.
///
/// On each call it captures the wire path from `req.uri().path()` *before* delegating,
/// then — when the inner response resolves — reads `grpc-status` from the response headers
/// and records `(wire_method, status_code)`. The inner service's request and response flow
/// through untouched; only headers are inspected. Generic over the request body `B` and
/// inner service `S` so it composes anywhere in tonic 0.11's server stack (which drives
/// `http::Request<Body>` → `http::Response<ResBody>`).
#[derive(Debug, Clone)]
pub struct WireCoverageService<S> {
    /// The wrapped tonic service the request is delegated to unchanged.
    inner: S,
    /// The shared recorder, cloned from the layer; incremented once per completed call.
    recorder: Arc<WireCoverageRecorder>,
}

impl<S, B, ResBody> Service<Request<B>> for WireCoverageService<S>
where
    S: Service<Request<B>, Response = Response<ResBody>>,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Readiness is purely the inner service's concern; the recorder never exerts
        // backpressure, so this is a transparent delegation.
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        // Capture the wire path as an owned String before the request is moved into the
        // inner service: `req.uri().path()` is the true `/package.Service/Method` the
        // suite drove, and it is exactly the key `coverage::resolve` consumes.
        let wire_method = req.uri().path().to_owned();
        let recorder = Arc::clone(&self.recorder);
        let future = self.inner.call(req);

        Box::pin(async move {
            let response = future.await?;
            // Inspect only the headers; the response (including its body) is returned
            // unchanged so streaming semantics and latency are unaffected.
            recorder.record(&wire_method, grpc_status_code(response.headers()));
            Ok(response)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        convert::Infallible,
        task::{Context, Poll},
    };

    use tonic::codegen::http::{HeaderMap, HeaderValue, Request, Response};

    use super::*;

    const START: &str = "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution";

    #[test]
    fn grpc_status_absent_header_is_ok() {
        let headers = HeaderMap::new();
        assert_eq!(grpc_status_code(&headers), 0);
    }

    #[test]
    fn grpc_status_parses_explicit_code() {
        let mut headers = HeaderMap::new();
        headers.insert(GRPC_STATUS_HEADER, HeaderValue::from_static("5"));
        assert_eq!(grpc_status_code(&headers), 5);
    }

    #[test]
    fn grpc_status_unparsable_header_degrades_to_ok() {
        let mut headers = HeaderMap::new();
        headers.insert(GRPC_STATUS_HEADER, HeaderValue::from_static("not-a-number"));
        assert_eq!(grpc_status_code(&headers), 0);
    }

    /// Minimal inner tonic-shaped service that returns a response carrying a preset
    /// `grpc-status` header, so the layer can be exercised end-to-end without a transport.
    #[derive(Clone)]
    struct StubService {
        status_code: Option<&'static str>,
    }

    impl Service<Request<()>> for StubService {
        type Response = Response<()>;
        type Error = Infallible;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Request<()>) -> Self::Future {
            let status_code = self.status_code;
            Box::pin(async move {
                let mut response = Response::new(());
                if let Some(code) = status_code {
                    response
                        .headers_mut()
                        .insert(GRPC_STATUS_HEADER, HeaderValue::from_static(code));
                }
                Ok(response)
            })
        }
    }

    #[tokio::test]
    async fn layer_records_wire_method_and_status_from_response_header() {
        let recorder = Arc::new(WireCoverageRecorder::new());
        let layer = WireCoverageLayer::new(Arc::clone(&recorder));
        let mut service = layer.layer(StubService {
            status_code: Some("5"),
        });

        let request = Request::builder()
            .uri(START)
            .body(())
            .expect("request builds");
        let _ = service
            .call(request)
            .await
            .expect("inner service is infallible");

        let record = recorder.snapshot();
        assert_eq!(record.rows.len(), 1);
        assert_eq!(record.rows[0].wire_method, START);
        assert_eq!(record.rows[0].status_code, 5);
        assert_eq!(record.rows[0].count, 1);
    }

    #[tokio::test]
    async fn layer_records_ok_when_response_has_no_grpc_status() {
        let recorder = Arc::new(WireCoverageRecorder::new());
        let layer = WireCoverageLayer::new(Arc::clone(&recorder));
        let mut service = layer.layer(StubService { status_code: None });

        let request = Request::builder()
            .uri(START)
            .body(())
            .expect("request builds");
        let _ = service
            .call(request)
            .await
            .expect("inner service is infallible");

        let record = recorder.snapshot();
        assert_eq!(record.rows.len(), 1);
        assert_eq!(record.rows[0].status_code, 0);
    }
}
