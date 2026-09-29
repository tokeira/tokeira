//! Bound outgoing gRPC status messages at the transport boundary.
//!
//! Both initial headers and trailing status frames pass here, including errors
//! produced by tonic before a handler is entered. Response data, status codes,
//! structured details, and other metadata pass through unchanged.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use http::{HeaderMap, Request, Response};
use http_body_util::BodyExt as _;
use tonic::{Status, body::Body, server::NamedService};
use tower::Service;

const MAX_MESSAGE_BYTES: usize = 4_000;
const TRUNCATED_SUFFIX: &str = "... <truncated>";

fn limited_status(status: Status) -> Status {
    let message = status.message();
    if message.len() <= MAX_MESSAGE_BYTES {
        return status;
    }
    let mut end = MAX_MESSAGE_BYTES - TRUNCATED_SUFFIX.len();
    // Back up only to a code-point boundary, never pad the result to 4000
    // (common/util/strings.go:9-20 and
    // common/rpc/interceptor/service_error_interceptor.go:54-60 @ v1.32.0).
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    Status::with_details_and_metadata(
        status.code(),
        format!("{}{}", &message[..end], TRUNCATED_SUFFIX),
        status.details().to_vec().into(),
        status.metadata().clone(),
    )
}

fn limit_headers(headers: &mut HeaderMap) {
    let Some(status) = Status::from_header_map(headers) else {
        return;
    };
    if status.message().len() <= MAX_MESSAGE_BYTES {
        return;
    }
    let mut encoded = HeaderMap::new();
    if limited_status(status).add_header(&mut encoded).is_ok()
        && let Some(message) = encoded.remove("grpc-message")
    {
        // Replace this header alone: grpc-status-details-bin must remain byte
        // identical, even when its embedded message differs from the outer one.
        headers.insert("grpc-message", message);
    }
}

/// A transparent service wrapper applying the Temporal status-message bound.
#[derive(Clone, Debug)]
pub struct StatusMessageLimit<S> {
    inner: S,
}

impl<S> StatusMessageLimit<S> {
    /// Wrap a gRPC service without changing its service name or response data.
    pub fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S: NamedService> NamedService for StatusMessageLimit<S> {
    const NAME: &'static str = S::NAME;
}

impl<S> Service<Request<Body>> for StatusMessageLimit<S>
where
    S: Service<Request<Body>, Response = Response<Body>>,
    S::Future: Send + 'static,
    S::Error: 'static,
{
    type Response = Response<Body>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let response = self.inner.call(request);
        Box::pin(async move {
            let (mut parts, body) = response.await?.into_parts();
            limit_headers(&mut parts.headers);
            let body = body.map_frame(|mut frame| {
                if let Some(trailers) = frame.trailers_mut() {
                    limit_headers(trailers);
                }
                frame
            });
            Ok(Response::from_parts(parts, Body::new(body)))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use proptest::prelude::*;
    use std::convert::Infallible;
    use tonic::{Code, metadata::MetadataMap};

    #[tokio::test]
    async fn service_limits_initial_and_trailing_status_without_changing_details() {
        for trailing in [false, true] {
            let original = Status::with_details(
                Code::InvalidArgument,
                "🦀".repeat(1100),
                Bytes::from_static(b"structured-details"),
            );
            let mut headers = HeaderMap::new();
            original.add_header(&mut headers).unwrap();
            let details = headers.get("grpc-status-details-bin").cloned();
            let mut service = StatusMessageLimit::new(tower::service_fn(move |_request| {
                let headers = headers.clone();
                async move {
                    let response = if trailing {
                        let body = http_body_util::Empty::<Bytes>::new()
                            .with_trailers(async move { Some(Ok::<_, Infallible>(headers)) });
                        Response::new(Body::new(body))
                    } else {
                        let mut response = Response::new(Body::empty());
                        *response.headers_mut() = headers;
                        response
                    };
                    Ok::<_, Infallible>(response)
                }
            }));
            let response = service.call(Request::new(Body::empty())).await.unwrap();
            let headers = if trailing {
                response
                    .into_body()
                    .collect()
                    .await
                    .unwrap()
                    .trailers()
                    .unwrap()
                    .clone()
            } else {
                response.headers().clone()
            };
            let limited = Status::from_header_map(&headers).unwrap();
            assert!(limited.message().len() <= MAX_MESSAGE_BYTES);
            assert!(limited.message().ends_with(TRUNCATED_SUFFIX));
            assert_eq!(limited.code(), Code::InvalidArgument);
            assert_eq!(headers.get("grpc-status-details-bin"), details.as_ref());
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        // Feature: v132-lifecycle-fidelity, Property 12: status message truncation
        // Compare a code-point model, preserving details and metadata exactly
        // (common/util/strings.go:9-20; common/rpc/interceptor/service_error_interceptor.go:57-60 @ v1.32.0).
        #[test]
        fn status_message_truncation(chars in prop::collection::vec(any::<char>(), 0..2200), code in 0i32..17,
            details in prop::collection::vec(any::<u8>(), 0..80)) {
            let message: String = chars.into_iter().collect();
            let mut metadata = MetadataMap::new();
            metadata.insert("test-metadata", "preserved".parse().unwrap());
            let status = Status::with_details_and_metadata(Code::from_i32(code), &message, details.clone().into(), metadata.clone());
            let mut headers = HeaderMap::new();
            status.add_header(&mut headers).unwrap();
            let previous_details = headers.get("grpc-status-details-bin").cloned();
            limit_headers(&mut headers);
            let result = Status::from_header_map(&headers).unwrap();
            let expected = if message.len() > 4000 {
                let mut bytes = 0;
                let prefix: String = message.chars().take_while(|ch| {
                    bytes += ch.len_utf8();
                    bytes <= 4000 - "... <truncated>".len()
                }).collect();
                prefix + "... <truncated>"
            } else { message };
            prop_assert_eq!(result.message(), expected);
            prop_assert!(result.message().len() <= 4000);
            prop_assert_eq!(result.code(), Code::from_i32(code));
            prop_assert_eq!(result.details(), details.as_slice());
            prop_assert_eq!(headers.get("grpc-status-details-bin"), previous_details.as_ref());
            prop_assert_eq!(result.metadata().get("test-metadata"), metadata.get("test-metadata"));
        }
    }
}
