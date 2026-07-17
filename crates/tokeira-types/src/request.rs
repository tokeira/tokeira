//! Request context carrying caller identity and idempotency keys.
//!
//! Every inbound gRPC call is assigned a `RequestId` at the edge. Storage uses
//! this identity for request deduplication so that retried or replayed calls
//! produce the same outcome without re-executing side effects.

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

/// Server-computed identity attributed to history events authored by a request.
///
/// This type is deliberately transport-neutral: the edge derives it from the
/// authenticated caller, while the kernel and storage carry it without taking
/// a dependency on the authentication implementation. A principal is absent
/// only when both fields are empty, matching Temporal's `GetPrincipal`
/// semantics (`common/headers/headers.go:148-162 @ v1.31.0`).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EventPrincipal {
    /// Authentication mechanism, such as `jwt` or `aws-iam`.
    pub principal_type: String,
    /// Authenticated subject name.
    pub name: String,
}

impl EventPrincipal {
    /// Return whether the principal contains no attributable identity.
    ///
    /// A half-empty value is intentionally not empty: an authenticated JWT
    /// with an empty `sub` still attributes `{type: "jwt", name: ""}`.
    pub fn is_empty(&self) -> bool {
        self.principal_type.is_empty() && self.name.is_empty()
    }
}

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
    /// Server-computed caller identity eligible for durable event attribution.
    ///
    /// No public request field populates this value; only the authenticated
    /// edge may set it. Keeping it distinct from `caller_identity` prevents a
    /// worker-supplied display identity from becoming an audit principal.
    #[serde(default)]
    pub principal: Option<EventPrincipal>,
    /// Wall-clock time when the edge received the request.
    ///
    /// Used for staleness checks and audit logging, not for
    /// ordering guarantees.
    pub received_at: OffsetDateTime,
}

impl RequestContext {
    /// Construct context for a server-originated command that has no caller.
    ///
    /// The fixed request id is safe only for command families whose idempotency
    /// is fenced by task sequence or timer identity rather than `RequestId`.
    /// Keeping that restriction explicit prevents internal work from acquiring
    /// a fabricated audit principal merely to satisfy a shared command shape.
    pub fn unattributed(received_at: OffsetDateTime) -> Self {
        Self {
            request_id: RequestId("internal-unattributed".to_owned()),
            caller_identity: None,
            principal: None,
            received_at,
        }
    }
}
