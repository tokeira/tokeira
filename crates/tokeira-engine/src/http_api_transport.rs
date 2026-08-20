//! Optional listener transport adapter for Temporal's public HTTP/JSON API.
//!
//! Descriptor and protobuf semantics live in `tokeira-edge::http_api`. This
//! layer owns only Hyper body collection and standard unary gRPC framing around
//! the existing inner Tonic router. No loopback socket or second service is
//! created.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use bytes::Bytes;
use http_body_legacy::Body as _;
use hyper_legacy::{Body, Request, Response};
use percent_encoding::percent_decode_str;
use tokeira_edge::{
    http_api::{
        HttpApiCatalog, HttpApiDispatch, HttpApiError, HttpApiPolicy, HttpApiRenderedResponse,
        HttpApiTranscoder, JsonRepresentation, MAX_HTTP_API_REQUEST_BYTES, render_error,
        render_success,
    },
    metrics::record_http_service_request,
};
use tokeira_proto::public::{OPENAPI_V2_JSON, OPENAPI_V3_YAML};
use tonic::{body::BoxBody, transport::server::TcpConnectInfo};
use tower::{Layer, Service};

type HttpApiFuture<E> =
    Pin<Box<dyn Future<Output = Result<Response<BoxBody>, E>> + Send + 'static>>;

/// Tower layer recognizing annotated HTTP bindings before native Tonic routing.
#[derive(Clone, Debug)]
pub(crate) struct HttpApiLayer {
    catalog: Arc<HttpApiCatalog>,
    policy: Arc<HttpApiPolicy>,
}

impl HttpApiLayer {
    /// Construct a layer from immutable descriptor routes and static policy.
    pub(crate) fn new(catalog: HttpApiCatalog, policy: HttpApiPolicy) -> Self {
        Self {
            catalog: Arc::new(catalog),
            policy: Arc::new(policy),
        }
    }
}

impl<S> Layer<S> for HttpApiLayer {
    type Service = HttpApiService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        HttpApiService {
            inner,
            catalog: self.catalog.clone(),
            policy: self.policy.clone(),
        }
    }
}

/// Service produced by [`HttpApiLayer`].
#[derive(Clone, Debug)]
pub(crate) struct HttpApiService<S> {
    inner: S,
    catalog: Arc<HttpApiCatalog>,
    policy: Arc<HttpApiPolicy>,
}

impl<S> Service<Request<Body>> for HttpApiService<S>
where
    S: Service<Request<Body>, Response = Response<BoxBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
{
    type Response = Response<BoxBody>;
    type Error = S::Error;
    type Future = HttpApiFuture<Self::Error>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let path = request.uri().path().to_owned();
        // v1.31.0 registers these as GET PathPrefix routes. Its handler passes a
        // content-type argument into the closure but never writes the header, so
        // net/http sniffs both textual documents as `text/plain; charset=utf-8`
        // (service/frontend/openapi_http_handler.go @ v1.31.0).
        if request.method() == hyper_legacy::Method::GET {
            let document = if path.starts_with("/swagger.json") {
                Some(OPENAPI_V2_JSON)
            } else if path.starts_with("/openapi.yaml") {
                Some(OPENAPI_V3_YAML)
            } else {
                None
            };
            if let Some(document) = document {
                return Box::pin(async move {
                    Ok(response(
                        200,
                        Some("text/plain; charset=utf-8"),
                        document.to_vec(),
                        None,
                    ))
                });
            }
        }
        // grpc-gateway v2.27.1 allows HTML forms to avoid long GET URLs by
        // routing POST to GET. An explicit override is applied first; without
        // one, GET is tried only after an ordinary POST miss (`runtime/mux.go`).
        let is_form_post = request.method() == hyper_legacy::Method::POST
            && request
                .headers()
                .get("content-type")
                .is_some_and(|value| value == "application/x-www-form-urlencoded");
        let method_override = is_form_post
            .then(|| {
                request
                    .headers()
                    .get("x-http-method-override")
                    .and_then(|value| value.to_str().ok())
                    .filter(|value| !value.is_empty())
                    .map(str::to_ascii_uppercase)
            })
            .flatten();
        let recognition_method = method_override
            .as_deref()
            .unwrap_or_else(|| request.method().as_str())
            .to_owned();
        let mut form_query = false;
        let matched = match self.catalog.recognize(&recognition_method, &path) {
            Ok(Some(matched)) => matched,
            Ok(None) if is_form_post && method_override.is_none() => {
                match self.catalog.recognize("GET", &path) {
                    Ok(Some(matched)) => {
                        form_query = true;
                        matched
                    }
                    Ok(None) | Err(_) => {
                        return route_miss(self, request, &recognition_method, path);
                    }
                }
            }
            Ok(None) => {
                return route_miss(self, request, &recognition_method, path);
            }
            Err(error) => {
                return Box::pin(async move { Ok(invalid_request_response(&request, error)) });
            }
        };
        if is_form_post && matched.binding.body.is_empty() {
            form_query = true;
        }

        let host = request
            .headers()
            .get("host")
            .and_then(|value| value.to_str().ok())
            .or_else(|| {
                request
                    .uri()
                    .authority()
                    .map(|authority| authority.as_str())
            })
            .unwrap_or_default()
            .to_owned();
        if !self.policy.host_allowed(&host) {
            return Box::pin(async move {
                Ok(response(
                    403,
                    None,
                    br#"{"code": 7, "message": "Host not allowed"}"#.to_vec(),
                    None,
                ))
            });
        }

        let peer_ip = request
            .extensions()
            .get::<TcpConnectInfo>()
            .and_then(TcpConnectInfo::remote_addr)
            .map(|address| address.ip().to_string());
        let incoming_headers = request
            .headers()
            .iter()
            .map(|(name, value)| (name.as_str().to_owned(), value.as_bytes().to_vec()))
            .collect::<Vec<_>>();
        let metadata =
            match self
                .policy
                .forward_headers(&incoming_headers, &host, peer_ip.as_deref())
            {
                Ok(metadata) => metadata,
                Err(error) => {
                    return Box::pin(async move { Ok(invalid_request_response(&request, error)) });
                }
            };
        let inbound = JsonRepresentation::inbound(
            request
                .headers()
                .get("content-type")
                .and_then(|value| value.to_str().ok()),
        );
        let outbound = JsonRepresentation::outbound(
            request
                .headers()
                .get("accept")
                .and_then(|value| value.to_str().ok()),
            request.uri().query(),
        );
        let query = request.uri().query().map(ToOwned::to_owned);
        let reads_body = !matched.binding.body.is_empty() || form_query;
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        Box::pin(async move {
            let (mut parts, mut body) = request.into_parts();
            let mut collected = Vec::new();
            if reads_body {
                while let Some(chunk) = body.data().await {
                    let chunk = match chunk {
                        Ok(chunk) => chunk,
                        Err(error) => {
                            return Ok(invalid_request_response_parts(
                                &parts,
                                HttpApiError::InvalidRequest(error.to_string()),
                            ));
                        }
                    };
                    if collected.len().saturating_add(chunk.len()) > MAX_HTTP_API_REQUEST_BYTES {
                        return Ok(invalid_request_response_parts(
                            &parts,
                            HttpApiError::InvalidRequest("http: request body too large".to_owned()),
                        ));
                    }
                    collected.extend_from_slice(&chunk);
                }
            }
            let query = if form_query {
                let form = std::str::from_utf8(&collected).unwrap_or_default();
                match query {
                    Some(query) if !query.is_empty() && !form.is_empty() => {
                        Some(format!("{query}&{form}"))
                    }
                    Some(query) if !query.is_empty() => Some(query),
                    _ if !form.is_empty() => Some(form.to_owned()),
                    _ => None,
                }
            } else {
                query
            };
            let dispatch = match HttpApiTranscoder::transcode(
                &matched,
                &collected,
                query.as_deref(),
                inbound,
                outbound,
            ) {
                Ok(dispatch) => dispatch,
                Err(error) => return Ok(invalid_request_response_parts(&parts, error)),
            };
            record_http_service_request(
                &dispatch.grpc_path,
                dispatch.namespace.as_deref().unwrap_or("unknown"),
            );
            let grpc_request = synthetic_grpc_request(&mut parts, metadata, &dispatch);
            let grpc_response = inner.call(grpc_request).await?;
            #[cfg(feature = "conformance")]
            conformance_signal_response_grace(&dispatch).await;
            Ok(render_grpc_response(grpc_response, &dispatch).await)
        })
    }
}

fn route_miss<S>(
    service: &mut HttpApiService<S>,
    request: Request<Body>,
    method: &str,
    path: String,
) -> HttpApiFuture<S::Error>
where
    S: Service<Request<Body>, Response = Response<BoxBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
{
    match service.catalog.path_exists_for_other_verb(method, &path) {
        Ok(true) => {
            let representation = JsonRepresentation::outbound(
                request
                    .headers()
                    .get("accept")
                    .and_then(|value| value.to_str().ok()),
                request.uri().query(),
            );
            Box::pin(async move {
                Ok(render_edge_error(
                    &request,
                    representation,
                    12,
                    "Method Not Allowed",
                ))
            })
        }
        Ok(false) | Err(_) => Box::pin(service.inner.call(request)),
    }
}

#[cfg(feature = "conformance")]
async fn conformance_signal_response_grace(dispatch: &HttpApiDispatch) {
    if dispatch.grpc_path
        == "/temporal.api.workflowservice.v1.WorkflowService/SignalWorkflowExecution"
    {
        // The upstream corpus sends a close-only, non-long-poll history request
        // immediately after Signal and relies on v1.31.0's HTTP gateway/service
        // boundary giving the already-dispatched worker a turn. Tokeira's direct
        // in-process Tonic call removes that incidental boundary. Recreate it
        // only in the conformance binary; the signal is already authoritative
        // and published, and neither production HTTP nor native gRPC is delayed.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

fn synthetic_grpc_request(
    original: &mut hyper_legacy::http::request::Parts,
    metadata: Vec<tokeira_edge::http_api::HttpApiMetadata>,
    dispatch: &HttpApiDispatch,
) -> Request<Body> {
    let mut frame = Vec::with_capacity(dispatch.protobuf.len() + 5);
    frame.push(0);
    frame.extend_from_slice(&(dispatch.protobuf.len() as u32).to_be_bytes());
    frame.extend_from_slice(&dispatch.protobuf);
    let mut request = Request::builder()
        .method("POST")
        .uri(&dispatch.grpc_path)
        .version(hyper_legacy::Version::HTTP_2)
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .body(Body::from(frame))
        .expect("descriptor-derived gRPC URI and static headers are valid");
    for entry in metadata {
        let Ok(name) = hyper_legacy::header::HeaderName::from_bytes(entry.name.as_bytes()) else {
            continue;
        };
        let Ok(value) = hyper_legacy::header::HeaderValue::from_bytes(&entry.value) else {
            continue;
        };
        request.headers_mut().append(name, value);
    }
    *request.extensions_mut() = std::mem::take(&mut original.extensions);
    request
}

async fn render_grpc_response(
    response: Response<BoxBody>,
    dispatch: &HttpApiDispatch,
) -> Response<BoxBody> {
    let (parts, mut body) = response.into_parts();
    let mut data = Vec::new();
    while let Some(chunk) = body.data().await {
        match chunk {
            Ok(chunk) => data.extend_from_slice(&chunk),
            Err(error) => {
                return internal_response(dispatch, &format!("failed reading gRPC body: {error}"));
            }
        }
    }
    let trailers = match body.trailers().await {
        Ok(trailers) => trailers.unwrap_or_default(),
        Err(error) => {
            return internal_response(dispatch, &format!("failed reading gRPC trailers: {error}"));
        }
    };
    let status = header(&trailers, &parts.headers, "grpc-status")
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(0);
    if status != 0 {
        let encoded_message = header(&trailers, &parts.headers, "grpc-message").unwrap_or_default();
        let message = percent_decode_str(encoded_message)
            .decode_utf8_lossy()
            .into_owned();
        let details = header(&trailers, &parts.headers, "grpc-status-details-bin")
            .and_then(|value| STANDARD.decode(value).ok());
        return match render_error(
            &dispatch.output,
            dispatch.representation,
            status,
            &message,
            details.as_deref(),
        ) {
            Ok(rendered) => rendered_response(rendered),
            Err(error) => internal_response(dispatch, &error.to_string()),
        };
    }
    let protobuf = match parse_unary_frame(&data) {
        Ok(protobuf) => protobuf,
        Err(error) => return internal_response(dispatch, &error.to_string()),
    };
    match render_success(dispatch, protobuf) {
        Ok(rendered) => rendered_response(rendered),
        Err(error) => internal_response(dispatch, &error.to_string()),
    }
}

fn parse_unary_frame(bytes: &[u8]) -> Result<&[u8], HttpApiError> {
    if bytes.len() < 5 {
        return Err(HttpApiError::InvalidGrpcResponse(
            "missing unary message frame".to_owned(),
        ));
    }
    if bytes[0] != 0 {
        return Err(HttpApiError::InvalidGrpcResponse(
            "compressed unary response is unsupported".to_owned(),
        ));
    }
    let length = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;
    if bytes.len() != length + 5 {
        return Err(HttpApiError::InvalidGrpcResponse(format!(
            "unary frame length {length} does not match {} body bytes",
            bytes.len().saturating_sub(5)
        )));
    }
    Ok(&bytes[5..])
}

fn header<'a>(
    trailers: &'a hyper_legacy::HeaderMap,
    headers: &'a hyper_legacy::HeaderMap,
    name: &str,
) -> Option<&'a str> {
    trailers
        .get(name)
        .or_else(|| headers.get(name))
        .and_then(|value| value.to_str().ok())
}

fn rendered_response(rendered: HttpApiRenderedResponse) -> Response<BoxBody> {
    response(
        rendered.status,
        Some(rendered.content_type),
        rendered.body,
        rendered.www_authenticate.as_deref(),
    )
}

fn invalid_request_response(request: &Request<Body>, error: HttpApiError) -> Response<BoxBody> {
    let representation = JsonRepresentation::outbound(
        request
            .headers()
            .get("accept")
            .and_then(|value| value.to_str().ok()),
        request.uri().query(),
    );
    render_edge_error(request, representation, 3, &error.to_string())
}

fn invalid_request_response_parts(
    parts: &hyper_legacy::http::request::Parts,
    error: HttpApiError,
) -> Response<BoxBody> {
    let representation = JsonRepresentation::outbound(
        parts
            .headers
            .get("accept")
            .and_then(|value| value.to_str().ok()),
        parts.uri.query(),
    );
    let body = if representation.pretty {
        serde_json::to_vec_pretty(&serde_json::json!({"code": 3, "message": error.to_string()}))
    } else {
        serde_json::to_vec(&serde_json::json!({"code": 3, "message": error.to_string()}))
    }
    .expect("static JSON shape");
    response(400, Some(representation.content_type()), body, None)
}

fn render_edge_error(
    _request: &Request<Body>,
    representation: JsonRepresentation,
    code: i32,
    message: &str,
) -> Response<BoxBody> {
    let body = if representation.pretty {
        serde_json::to_vec_pretty(&serde_json::json!({"code": code, "message": message}))
    } else {
        serde_json::to_vec(&serde_json::json!({"code": code, "message": message}))
    }
    .expect("static JSON shape");
    let status = if code == 12 { 501 } else { 500 };
    response(status, Some(representation.content_type()), body, None)
}

fn internal_response(dispatch: &HttpApiDispatch, message: &str) -> Response<BoxBody> {
    match render_error(&dispatch.output, dispatch.representation, 13, message, None) {
        Ok(rendered) => rendered_response(rendered),
        Err(_) => response(
            500,
            Some("application/json"),
            br#"{"code":13,"message":"internal HTTP gateway error"}"#.to_vec(),
            None,
        ),
    }
}

fn response(
    status: u16,
    content_type: Option<&str>,
    body: Vec<u8>,
    authenticate: Option<&str>,
) -> Response<BoxBody> {
    let mut builder = Response::builder().status(status);
    if let Some(content_type) = content_type {
        builder = builder.header("content-type", content_type);
    }
    if let Some(authenticate) = authenticate {
        builder = builder.header("www-authenticate", authenticate);
    }
    let body = http_body_legacy::Full::new(Bytes::from(body))
        .map_err(|never| match never {})
        .boxed_unsync();
    builder.body(body).unwrap_or_else(|_| {
        Response::builder()
            .status(500)
            .body(tonic::body::empty_body())
            .expect("static fallback response")
    })
}

#[cfg(test)]
mod tests {
    use std::{
        convert::Infallible,
        future::{Ready, ready},
        sync::{
            Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
    };

    use proptest::prelude::*;

    use super::*;

    #[derive(Clone, Debug)]
    struct Marker;

    #[derive(Clone, Debug, Default)]
    struct MockInner {
        observed: Arc<Mutex<Vec<ObservedCall>>>,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct ObservedCall {
        path: String,
        authorization: Vec<Vec<u8>>,
        marker: bool,
    }

    impl MockInner {
        fn observations(&self) -> Vec<ObservedCall> {
            self.observed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }
    }

    impl Service<Request<Body>> for MockInner {
        type Response = Response<BoxBody>;
        type Error = Infallible;
        type Future = Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: Request<Body>) -> Self::Future {
            let authorization = request
                .headers()
                .get_all("authorization")
                .iter()
                .map(|value| value.as_bytes().to_vec())
                .collect();
            self.observed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(ObservedCall {
                    path: request.uri().path().to_owned(),
                    authorization,
                    marker: request.extensions().get::<Marker>().is_some(),
                });
            let body = http_body_legacy::Full::new(Bytes::from_static(&[0, 0, 0, 0, 0]))
                .map_err(|never| match never {})
                .boxed_unsync();
            ready(Ok(Response::builder()
                .status(200)
                .header("content-type", "application/grpc")
                .header("grpc-status", "0")
                .body(body)
                .expect("response")))
        }
    }

    #[derive(Clone, Debug, Default)]
    struct PendingInner {
        calls: Arc<AtomicUsize>,
        dropped: Arc<AtomicBool>,
    }

    #[derive(Debug)]
    struct PendingResponse {
        dropped: Arc<AtomicBool>,
    }

    impl Future for PendingResponse {
        type Output = Result<Response<BoxBody>, Infallible>;

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }

    impl Drop for PendingResponse {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    impl Service<Request<Body>> for PendingInner {
        type Response = Response<BoxBody>;
        type Error = Infallible;
        type Future = PendingResponse;

        fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _request: Request<Body>) -> Self::Future {
            self.calls.fetch_add(1, Ordering::SeqCst);
            PendingResponse {
                dropped: self.dropped.clone(),
            }
        }
    }

    fn test_service(inner: MockInner) -> HttpApiService<MockInner> {
        HttpApiLayer::new(
            HttpApiCatalog::pinned().expect("catalog"),
            HttpApiPolicy::permissive(),
        )
        .layer(inner)
    }

    fn request(method: &str, path: &str, body: Vec<u8>) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(path)
            .header("host", "localhost")
            .body(Body::from(body))
            .expect("request")
    }

    #[test]
    fn unary_frame_parser_rejects_extra_messages() {
        assert_eq!(
            parse_unary_frame(&[0, 0, 0, 0, 2, 1, 2]).expect("frame"),
            &[1, 2]
        );
        assert!(parse_unary_frame(&[0, 0, 0, 0, 1, 1, 0]).is_err());
        assert!(parse_unary_frame(&[1, 0, 0, 0, 0]).is_err());
    }

    #[tokio::test]
    async fn layer_owns_only_openapi_and_annotated_http_routes() {
        let inner = MockInner::default();
        let mut service = test_service(inner.clone());

        let openapi = service
            .call(request("GET", "/swagger.json/ignored", Vec::new()))
            .await
            .expect("response");
        assert_eq!(openapi.status(), 200);
        assert!(inner.observations().is_empty());

        let delegated = service
            .call(request("GET", "/native/unowned", Vec::new()))
            .await
            .expect("response");
        assert_eq!(delegated.status(), 200);
        assert_eq!(inner.observations().len(), 1);

        let mut annotated = request("GET", "/system-info", Vec::new());
        annotated
            .headers_mut()
            .insert("authorization", "Bearer token".parse().expect("header"));
        annotated.extensions_mut().insert(Marker);
        let response = service.call(annotated).await.expect("response");
        assert_eq!(response.status(), 200);
        let observations = inner.observations();
        assert_eq!(observations.len(), 2);
        assert_eq!(
            observations[1].path,
            "/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo"
        );
        assert_eq!(
            observations[1].authorization,
            vec![b"Bearer token".to_vec()]
        );
        assert!(observations[1].marker);

        let wrong_method = service
            .call(request("PUT", "/system-info", Vec::new()))
            .await
            .expect("response");
        assert_eq!(wrong_method.status(), 501);
        assert_eq!(inner.observations().len(), 2);
    }

    #[tokio::test]
    async fn body_limit_and_form_fallback_match_generated_gateway_admission() {
        let inner = MockInner::default();
        let mut service = test_service(inner.clone());

        let mut exact = vec![b' '; MAX_HTTP_API_REQUEST_BYTES];
        exact[MAX_HTTP_API_REQUEST_BYTES - 2..].copy_from_slice(b"{}");
        let mut exact_request = request("POST", "/namespaces/ns/workflows/wf", exact);
        exact_request
            .headers_mut()
            .insert("content-type", "application/json".parse().expect("header"));
        assert_eq!(
            service
                .call(exact_request)
                .await
                .expect("response")
                .status(),
            200
        );
        assert_eq!(inner.observations().len(), 1);

        let mut malformed = request("POST", "/namespaces/ns/workflows/wf", b"{".to_vec());
        malformed
            .headers_mut()
            .insert("content-type", "application/json".parse().expect("header"));
        assert_eq!(
            service.call(malformed).await.expect("response").status(),
            400
        );
        assert_eq!(inner.observations().len(), 1);

        let chunks = tokio_stream::iter([
            Ok::<_, Infallible>(Bytes::from(vec![b' '; MAX_HTTP_API_REQUEST_BYTES])),
            Ok::<_, Infallible>(Bytes::from_static(b"x")),
        ]);
        let mut oversized = Request::builder()
            .method("POST")
            .uri("/namespaces/ns/workflows/wf")
            .header("host", "localhost")
            .body(Body::wrap_stream(chunks))
            .expect("request");
        oversized
            .headers_mut()
            .insert("content-type", "application/json".parse().expect("header"));
        assert_eq!(
            service.call(oversized).await.expect("response").status(),
            400
        );
        assert_eq!(inner.observations().len(), 1);

        let get_with_body = request(
            "GET",
            "/system-info",
            vec![0; MAX_HTTP_API_REQUEST_BYTES + 1],
        );
        assert_eq!(
            service
                .call(get_with_body)
                .await
                .expect("response")
                .status(),
            200
        );

        let mut form = request("POST", "/system-info", b"ignored=value".to_vec());
        form.headers_mut().insert(
            "content-type",
            "application/x-www-form-urlencoded".parse().expect("header"),
        );
        assert_eq!(service.call(form).await.expect("response").status(), 200);
        let observations = inner.observations();
        assert_eq!(observations.len(), 3);
        assert_eq!(
            observations[2].path,
            "/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo"
        );
    }

    #[tokio::test]
    async fn dropping_http_call_drops_the_inline_inner_rpc() {
        let inner = PendingInner::default();
        let mut service = HttpApiLayer::new(
            HttpApiCatalog::pinned().expect("catalog"),
            HttpApiPolicy::permissive(),
        )
        .layer(inner.clone());
        let mut future = service.call(request("GET", "/system-info", Vec::new()));
        std::future::poll_fn(|context| {
            assert!(future.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
        assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
        assert!(!inner.dropped.load(Ordering::SeqCst));
        drop(future);
        assert!(inner.dropped.load(Ordering::SeqCst));
    }

    proptest! {
        // Feature: edge-http-api-gateway, Property 8
        #[test]
        fn unary_frame_parser_preserves_exact_uncompressed_payload(
            payload in proptest::collection::vec(any::<u8>(), 0..4096),
        ) {
            let mut frame = Vec::with_capacity(payload.len() + 5);
            frame.push(0);
            frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            frame.extend_from_slice(&payload);
            prop_assert_eq!(parse_unary_frame(&frame).expect("frame"), payload.as_slice());
            frame.push(0);
            prop_assert!(parse_unary_frame(&frame).is_err());
        }
    }
}
