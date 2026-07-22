//! Temporal HTTP/JSON gateway translation at the compatibility edge.
//!
//! This module owns descriptor-derived route recognition and protobuf/JSON
//! transcoding. Hyper body collection and invocation of the concrete Tonic
//! router remain in `tokeirad`; workflow semantics remain in the existing edge
//! services and authoritative runtime.

mod json;
mod policy;
mod response;
mod route;
mod transcode;

pub use json::{JsonRepresentation, decode_json_message, encode_json_message};
pub use policy::{HttpApiMetadata, HttpApiPolicy};
pub use response::{HttpApiRenderedResponse, render_error, render_success};
pub use route::{HttpApiBinding, HttpApiCatalog, HttpApiMatch, HttpVerb, PathCapture};
pub use transcode::{HttpApiDispatch, HttpApiTranscoder};

/// Maximum body bytes read by a binding that declares an HTTP body.
///
/// Temporal applies `rpc.MaxHTTPAPIRequestBytes` through `http.MaxBytesReader`
/// (`service/frontend/http_api_server.go @ v1.31.0`). Generated no-body
/// handlers do not read merely to trigger this limit.
pub const MAX_HTTP_API_REQUEST_BYTES: usize = 4 * 1024 * 1024;

/// Failure while constructing or translating the public HTTP API.
#[derive(Debug, thiserror::Error)]
pub enum HttpApiError {
    /// The vendored descriptor set or an annotation is internally inconsistent.
    #[error("HTTP API descriptor error: {0}")]
    Descriptor(String),
    /// A caller supplied an invalid path, query, header, or JSON value.
    #[error("invalid HTTP API request: {0}")]
    InvalidRequest(String),
    /// The existing Tonic service returned an invalid unary gRPC envelope.
    #[error("invalid inner gRPC response: {0}")]
    InvalidGrpcResponse(String),
}
