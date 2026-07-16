//! Transport-neutral Workflow Rule records shared by edge, runtime, and storage.
//!
//! The public protobuf is translated into this model at the compatibility edge. Keeping the
//! durable record free of protobuf types lets storage persist it and the runtime evaluate it at
//! activity lifecycle boundaries without making either plane depend on the wire package.

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

/// One namespace-scoped Workflow Rule and its creation provenance.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowRuleRecord {
    /// Namespace-unique caller-supplied rule identifier.
    pub id: String,
    /// Server-assigned creation time.
    pub create_time: OffsetDateTime,
    /// Identity of the caller that created the rule.
    pub created_by_identity: String,
    /// Human-readable rule description.
    pub description: String,
    /// Trigger evaluated before an activity starts or retries.
    pub trigger: WorkflowRuleTrigger,
    /// Restricted visibility predicate evaluated before the activity predicate.
    pub visibility_query: String,
    /// Actions applied when both predicates match.
    pub actions: Vec<WorkflowRuleAction>,
    /// Time after which automatic evaluation ignores the rule.
    ///
    /// Expiration does not itself delete the record. Temporal v1.31.0 retains expired namespace
    /// entries for CRUD reads and considers them only for capacity eviction.
    pub expiration_time: Option<OffsetDateTime>,
}

/// Supported Workflow Rule trigger variants at the v1.31.0 compatibility target.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkflowRuleTrigger {
    /// Evaluate the SQL-like predicate against an activity about to start.
    ActivityStart {
        /// Activity predicate preserved exactly as supplied by the caller.
        predicate: String,
    },
    /// A newer or unknown trigger that v1.31.0 cannot execute.
    Unsupported,
}

/// Supported Workflow Rule action variants at the v1.31.0 compatibility target.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkflowRuleAction {
    /// Pause the matching activity before dispatch.
    ActivityPause,
    /// A newer or unknown action that v1.31.0 cannot execute.
    Unsupported,
}

impl WorkflowRuleRecord {
    /// Return whether automatic evaluation may consider this rule at `now`.
    pub fn is_unexpired_at(&self, now: OffsetDateTime) -> bool {
        self.expiration_time.is_none_or(|expiry| expiry > now)
    }

    /// Return whether this rule has an activity-pause action.
    pub fn pauses_activity(&self) -> bool {
        self.actions.contains(&WorkflowRuleAction::ActivityPause)
    }
}
