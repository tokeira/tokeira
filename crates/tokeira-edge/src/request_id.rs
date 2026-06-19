//! Request-id extraction and generation.
//!
//! A stable request id ties together ingress logs, downstream runtime logs, and the
//! client's own logs across a distributed call. The edge preserves a caller-supplied
//! [`REQUEST_ID_HEADER`] when present and mints a fresh one otherwise
//! ([`extract_or_generate`]), so correlation survives whether or not the client set
//! it. Generation is behind the small [`RequestIdGenerator`] trait because it can be
//! security-sensitive or trace-propagation-tied in some deployments; the edge itself
//! only needs "give me a fresh id".

use http::HeaderMap;
use uuid::Uuid;

/// Canonical request-id header used by the edge.
///
/// It is intentionally generic rather than transport-specific so the same code
/// can be reused for gRPC metadata and HTTP headers.
pub const REQUEST_ID_HEADER: &str = "x-request-id";

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RequestId(String);

impl RequestId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_inner(self) -> String {
        self.0
    }
}

/// Generator trait kept small on purpose.
///
/// Request ID generation is security-sensitive in some environments and can also
/// be tied into trace/span propagation. The edge only needs "give me a fresh id".
pub trait RequestIdGenerator: Send + Sync + 'static {
    fn generate(&self) -> RequestId;
}

#[derive(Debug, Default)]
pub struct UuidRequestIdGenerator;

impl RequestIdGenerator for UuidRequestIdGenerator {
    fn generate(&self) -> RequestId {
        RequestId(Uuid::new_v4().to_string())
    }
}

/// Prefer the caller's request id when present, otherwise mint a new one.
///
/// Preserving an incoming request id is extremely useful in distributed systems
/// because it ties together ingress logs, downstream runtime logs, and client logs.
pub fn extract_or_generate(headers: &HeaderMap, generator: &dyn RequestIdGenerator) -> RequestId {
    headers
        .get(REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
        .map(|value| RequestId::new(value.to_owned()))
        .unwrap_or_else(|| generator.generate())
}
