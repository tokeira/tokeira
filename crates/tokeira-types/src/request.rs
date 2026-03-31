use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

/// Explicit request identity used for idempotency and debugging.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RequestId(pub String);

/// Request-scoped context carried from the edge into the core system.
///
/// This is intentionally small. It should contain enough information to reason
/// about idempotency and causality without becoming a generic baggage object.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestContext {
    pub request_id: RequestId,
    pub caller_identity: Option<String>,
    pub received_at: OffsetDateTime,
}
