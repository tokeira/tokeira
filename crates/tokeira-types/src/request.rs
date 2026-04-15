//! Request context carrying caller identity and idempotency keys.
//!
//! Every inbound gRPC call is assigned a `RequestId` at the edge. Storage uses
//! this identity for request deduplication so that retried or replayed calls
//! produce the same outcome without re-executing side effects.

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

/// Explicit request identity used for idempotency and
/// debugging.
///
/// The edge layer assigns a `RequestId` to every incoming
/// gRPC call. Storage can use it to detect and deduplicate
/// retried requests that carry the same identity.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RequestId(pub String);

/// Request-scoped context carried from the edge into the core
/// system.
///
/// This is intentionally small. It should contain enough
/// information to reason about idempotency and causality
/// without becoming a generic baggage object. If you find
/// yourself wanting to add more fields, consider whether they
/// belong in [`Headers`](crate::Headers) instead.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestContext {
    /// Unique identifier for this request, used for dedup.
    pub request_id: RequestId,
    /// Optional identity of the caller (e.g. worker identity
    /// or service account). `None` for anonymous callers.
    pub caller_identity: Option<String>,
    /// Wall-clock time when the edge received the request.
    ///
    /// Used for staleness checks and audit logging, not for
    /// ordering guarantees.
    pub received_at: OffsetDateTime,
}
