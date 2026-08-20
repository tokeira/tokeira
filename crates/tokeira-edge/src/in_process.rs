//! Transport-neutral in-process dispatch for the Temporal gRPC services.
//!
//! The Rust SDK's callback transport exchanges a method name, metadata, and
//! unframed protobuf bytes. Tokeira's generated services still expect normal
//! gRPC HTTP requests, so this module owns the narrow framing bridge and invokes
//! the same tonic router used by the network server. Keeping the bridge at the
//! edge preserves admission, authorization, tracing, and status mapping without
//! coupling the engine to a particular SDK or tonic generation.

use std::sync::Arc;

use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue};
use http_body_legacy::Body as _;
use hyper_legacy::{Body, Request, Version};
use tokio::sync::Mutex;
use tonic::{Code, Status, transport::server::Routes};
use tower::ServiceExt as _;

use crate::grpc::{
    admin_service::AdminServiceGrpc, operator_service::OperatorServiceGrpc,
    workflow_service::WorkflowServiceGrpc,
};

/// One uncompressed unary gRPC call represented at the protobuf boundary.
#[derive(Clone, Debug)]
pub struct InProcessGrpcRequest {
    /// Fully qualified gRPC service name, without a leading slash.
    pub service: String,
    /// RPC method name.
    pub rpc: String,
    /// Request metadata in the SDK's `http` 1.x representation.
    pub headers: HeaderMap,
    /// Unframed protobuf request bytes.
    pub proto: Bytes,
}

/// One successful unary gRPC response represented at the protobuf boundary.
#[derive(Clone, Debug)]
pub struct InProcessGrpcResponse {
    /// Initial and trailing response metadata, excluding transport-owned gRPC fields.
    pub headers: HeaderMap,
    /// Unframed protobuf response bytes.
    pub proto: Vec<u8>,
}

/// Cloneable in-process router over Tokeira's Temporal services.
///
/// Calls execute inline. Dropping a caller's future therefore drops the edge
/// handler future as well, which is load-bearing for long-poll cancellation and
/// its RAII admission permits.
#[derive(Clone, Debug)]
pub struct InProcessGrpcService {
    // Tonic 0.11's Axum router is Send but not Sync. The mutex exists only to
    // clone its cheap service handles; it is released before any RPC future is
    // polled, so concurrent long polls do not serialize behind one another.
    routes: Arc<Mutex<Routes>>,
}

impl InProcessGrpcService {
    /// Assemble the same Workflow, Operator, and Admin tonic services the network
    /// listener mounts, without constructing or binding a transport.
    pub fn new(
        workflow: WorkflowServiceGrpc,
        operator: OperatorServiceGrpc,
        admin: AdminServiceGrpc,
    ) -> Self {
        let routes = Routes::new(workflow.into_service())
            .add_service(operator.into_service())
            .add_service(admin.into_service());
        Self {
            routes: Arc::new(Mutex::new(routes)),
        }
    }

    /// Dispatch one uncompressed unary call through the assembled tonic router.
    ///
    /// The returned [`Status`] is the edge adapter's original gRPC code, message,
    /// details, and metadata. No task is spawned around the call.
    pub async fn call(
        &self,
        request: InProcessGrpcRequest,
    ) -> Result<InProcessGrpcResponse, Status> {
        validate_method_name(&request.service, "service")?;
        validate_method_name(&request.rpc, "rpc")?;

        let path = format!("/{}/{}", request.service, request.rpc);
        let mut frame = Vec::with_capacity(request.proto.len() + 5);
        frame.push(0);
        let length = u32::try_from(request.proto.len()).map_err(|_| {
            Status::resource_exhausted("gRPC request exceeds the unary frame limit")
        })?;
        frame.extend_from_slice(&length.to_be_bytes());
        frame.extend_from_slice(&request.proto);

        let mut grpc_request = Request::builder()
            .method("POST")
            .uri(path)
            .version(Version::HTTP_2)
            .body(Body::from(frame))
            .map_err(|error| Status::invalid_argument(format!("invalid gRPC method: {error}")))?;
        copy_request_headers(&request.headers, grpc_request.headers_mut());
        // The SDK callback boundary explicitly does not support compression. Force
        // identity after metadata copying so a caller cannot accidentally make the
        // generated server return a compressed frame the callback cannot decode.
        grpc_request.headers_mut().insert(
            hyper_legacy::header::CONTENT_TYPE,
            hyper_legacy::header::HeaderValue::from_static("application/grpc"),
        );
        grpc_request.headers_mut().insert(
            hyper_legacy::header::TE,
            hyper_legacy::header::HeaderValue::from_static("trailers"),
        );
        grpc_request.headers_mut().remove("grpc-encoding");
        grpc_request.headers_mut().remove("grpc-accept-encoding");

        let routes = self.routes.lock().await.clone();
        let response = routes.oneshot(grpc_request).await.map_err(|error| {
            Status::internal(format!("in-process gRPC dispatch failed: {error}"))
        })?;
        let (parts, mut body) = response.into_parts();
        let mut framed = Vec::new();
        while let Some(chunk) = body.data().await {
            let chunk = chunk.map_err(|error| {
                Status::internal(format!("failed reading in-process gRPC response: {error}"))
            })?;
            framed.extend_from_slice(&chunk);
        }
        let trailers = body
            .trailers()
            .await
            .map_err(|error| {
                Status::internal(format!("failed reading in-process gRPC trailers: {error}"))
            })?
            .unwrap_or_default();

        let mut status_headers = parts.headers.clone();
        for (name, value) in &trailers {
            status_headers.append(name, value.clone());
        }
        if let Some(status) = Status::from_header_map(&status_headers)
            && status.code() != Code::Ok
        {
            return Err(status);
        }

        let proto = parse_unary_frame(&framed)?;
        let mut headers = HeaderMap::new();
        copy_response_headers(&parts.headers, &mut headers);
        copy_response_headers(&trailers, &mut headers);
        Ok(InProcessGrpcResponse { headers, proto })
    }
}

fn validate_method_name(value: &str, field: &str) -> Result<(), Status> {
    if value.is_empty() || value.contains('/') || value.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(Status::invalid_argument(format!(
            "invalid in-process gRPC {field} name"
        )));
    }
    Ok(())
}

fn copy_request_headers(source: &HeaderMap, target: &mut hyper_legacy::HeaderMap) {
    for (name, value) in source {
        let Ok(name) = hyper_legacy::header::HeaderName::from_bytes(name.as_str().as_bytes())
        else {
            continue;
        };
        let Ok(value) = hyper_legacy::header::HeaderValue::from_bytes(value.as_bytes()) else {
            continue;
        };
        target.append(name, value);
    }
}

fn copy_response_headers(source: &hyper_legacy::HeaderMap, target: &mut HeaderMap) {
    for (name, value) in source {
        if matches!(
            name.as_str(),
            "content-type" | "grpc-status" | "grpc-message" | "grpc-status-details-bin"
        ) {
            continue;
        }
        let Ok(name) = HeaderName::from_bytes(name.as_str().as_bytes()) else {
            continue;
        };
        let Ok(value) = HeaderValue::from_bytes(value.as_bytes()) else {
            continue;
        };
        target.append(name, value);
    }
}

fn parse_unary_frame(bytes: &[u8]) -> Result<Vec<u8>, Status> {
    if bytes.len() < 5 {
        return Err(Status::internal(
            "gRPC response omitted its unary message frame",
        ));
    }
    if bytes[0] != 0 {
        return Err(Status::internal(
            "compressed in-process gRPC responses are unsupported",
        ));
    }
    let length = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;
    if bytes.len() != length + 5 {
        return Err(Status::internal(format!(
            "gRPC unary frame length {length} does not match {} response bytes",
            bytes.len().saturating_sub(5)
        )));
    }
    Ok(bytes[5..].to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_survives_http_version_bridge() {
        let mut modern = HeaderMap::new();
        modern.append("x-request-id", HeaderValue::from_static("request-1"));
        modern.append("x-request-id", HeaderValue::from_static("request-2"));

        let mut legacy = hyper_legacy::HeaderMap::new();
        copy_request_headers(&modern, &mut legacy);
        let mut round_trip = HeaderMap::new();
        copy_response_headers(&legacy, &mut round_trip);

        let values = round_trip
            .get_all("x-request-id")
            .iter()
            .map(|value| value.to_str().expect("test metadata is ASCII"))
            .collect::<Vec<_>>();
        assert_eq!(values, ["request-1", "request-2"]);
    }

    #[test]
    fn response_bridge_removes_transport_owned_headers() {
        let mut legacy = hyper_legacy::HeaderMap::new();
        legacy.insert(
            "content-type",
            "application/grpc".parse().expect("static value"),
        );
        legacy.insert("grpc-status", "0".parse().expect("static value"));
        legacy.insert("x-engine", "embedded".parse().expect("static value"));

        let mut modern = HeaderMap::new();
        copy_response_headers(&legacy, &mut modern);

        assert_eq!(
            modern.get("x-engine").expect("metadata retained"),
            "embedded"
        );
        assert!(!modern.contains_key("content-type"));
        assert!(!modern.contains_key("grpc-status"));
    }
}
