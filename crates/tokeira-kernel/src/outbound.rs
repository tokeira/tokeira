//! Authoritative identities for outbound work and child-start handshakes.
//!
//! Runtime observations enter through fenced per-run transitions. These values
//! contain no delivery claims: loss of every executor leaves the same identities
//! and obligations. Storage encodes them in a required, versioned state extension.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tokeira_types::{NamespaceId, RequestId, RunId, RunKey, WorkflowId};

use crate::{ParentClosePolicy, Priority, WorkflowIdReusePolicy, WorkflowVersioningInfo};

/// Required outbound-state layout. A different layout requires a new version.
pub const OUTBOUND_STATE_VERSION: u32 = 1;

/// Stable operation slot; discriminants also identify physical index rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(i16)]
pub enum OperationKind {
    /// A child initiation awaiting a definitive start outcome.
    ChildStart = 1,
    /// A confirmed child whose first task has not been released.
    ChildRelease = 2,
    /// An external signal awaiting source confirmation.
    ExternalSignal = 3,
    /// An external cancellation awaiting source confirmation.
    ExternalCancel = 4,
    /// A scheduled or backing-off Nexus invocation.
    NexusStart = 5,
    /// A scheduled or backing-off Nexus cancellation attempt.
    NexusCancel = 6,
}

/// Stable identity of committed work, independent of a process or lease owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct OperationKey {
    /// Run whose committed state owns the obligation.
    pub source_run: RunKey,
    /// Distinguishes independent slots sharing an initiation event.
    pub kind: OperationKind,
    /// Initiation/schedule event in the source run.
    pub initiating_event: i64,
}

/// Incarnation captured before an attempt and checked again before its outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationIncarnation {
    /// Stable source slot.
    pub key: OperationKey,
    /// Cancel-request event, or zero for operations without that dimension.
    pub requested_event: i64,
    /// Recorded completed-attempt count; no clock-derived generation is used.
    pub attempt: u32,
}

/// Identity and inheritance selected once before the first child-start attempt.
///
/// Static accepted fields remain in the initiation event. The destination chooses
/// the actual creation clock; `prepared_at` must never consume a child's timeout
/// while source delivery is unavailable.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ChildStartSeed {
    /// Chosen execution reused after ambiguous replies and owner replacement.
    pub child_run_id: RunId,
    /// Stable, source-domain-separated start deduplication identity.
    pub request_id: RequestId,
    /// Diagnostic preparation time, not the child's start time.
    pub prepared_at: OffsetDateTime,
    /// Resolved parent namespace name captured with inheritance.
    pub parent_namespace_name: Option<String>,
    /// Root execution identity retained across child chains.
    pub root_workflow_id: Option<WorkflowId>,
    /// Root execution's first run identity.
    pub root_run_id: Option<RunId>,
    /// Accepted child override merged with the committed parent at preparation.
    pub effective_priority: Option<Priority>,
    /// Verified versioning inheritance selected before any target side effect.
    pub versioning_info: Option<WorkflowVersioningInfo>,
}

/// Confirmation proof retained until the child acknowledges first-task release.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChildReleaseState {
    /// Child's namespace; it can differ from the source namespace.
    pub namespace_id: NamespaceId,
    /// Child workflow whose chosen run was confirmed.
    pub workflow_id: WorkflowId,
    /// Parent initiation event, independent of a later reuse of the child ID.
    pub initiating_event: i64,
    /// Exact first child run whose start was confirmed.
    pub child_run_id: RunId,
    /// Durable parent Started event. Absent while its event is buffered behind
    /// an in-flight parent task; absence never authorizes child-task release.
    pub parent_started_event_id: Option<i64>,
    /// Determines whether release remains owed after parent close.
    pub parent_close_policy: ParentClosePolicy,
}

/// Child-side gate proving which parent confirmation can release its first task.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChildStartGate {
    /// Exact parent run, including when namespace/workflow IDs are reused.
    pub parent_run_key: RunKey,
    /// Parent initiation that created this child.
    pub initiating_event: i64,
    /// Child chain's first run; successors cannot impersonate another initiation.
    pub first_child_run_id: RunId,
    /// Set only after runtime verifies the committed parent Started event.
    pub released: bool,
}

/// Fences a final child result against the parent's pending initiation and chain.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChildOutcomeIdentity {
    /// Parent initiation event expected by the child result.
    pub parent_initiated_event: i64,
    /// First child run, retained through retries and continue-as-new.
    pub child_first_run: RunId,
    /// Final run that authored the terminal event.
    pub child_final_run: RunId,
}

/// Internal destination deduplication provenance retained while source work is owed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboundOrigin {
    /// Committed source incarnation; a caller cannot supply arbitrary payloads.
    pub incarnation: OperationIncarnation,
}

/// Required metadata whose absence in a legacy envelope needs checked preparation.
///
/// `WorkflowState::outbound == None` denotes legacy state, rather than an empty set
/// of new obligations. Runtime-only selections are not recoverable from public
/// history and therefore cannot be discarded by an old writer.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OutboundState {
    /// Exact layout version; unknown versions fail closed during decoding.
    pub version: u32,
    /// Winning child identity/inheritance, keyed by parent initiation.
    pub child_seeds: BTreeMap<i64, ChildStartSeed>,
    /// Confirmed children whose first task still needs verified release.
    pub child_releases: BTreeMap<i64, ChildReleaseState>,
    /// This run's own first-task gate, when it was created as a child.
    pub child_gate: Option<ChildStartGate>,
    /// Operator-verified policies missing from old initiation events.
    pub legacy_child_policies: BTreeMap<i64, WorkflowIdReusePolicy>,
    /// Existing first child runs verified during stopped-cluster preparation.
    /// This evidence permits adoption only, never a replacement start with an
    /// invented reuse policy or a fabricated delivery receipt.
    pub legacy_adopted_children: BTreeMap<i64, RunId>,
    /// Reset create-request identity, distinct from the copied original start ID.
    pub reset_request_id: Option<RequestId>,
}

impl Default for OutboundState {
    fn default() -> Self {
        Self {
            version: OUTBOUND_STATE_VERSION,
            child_seeds: BTreeMap::new(),
            child_releases: BTreeMap::new(),
            child_gate: None,
            legacy_child_policies: BTreeMap::new(),
            legacy_adopted_children: BTreeMap::new(),
            reset_request_id: None,
        }
    }
}

impl OutboundState {
    /// Whether the current envelope can represent this metadata without a section.
    /// Only a known new envelope can infer this empty value; a legacy envelope cannot.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.child_seeds.is_empty()
            && self.child_releases.is_empty()
            && self.child_gate.is_none()
            && self.legacy_child_policies.is_empty()
            && self.legacy_adopted_children.is_empty()
            && self.reset_request_id.is_none()
    }
}
