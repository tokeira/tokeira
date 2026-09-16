//! Durable resource state, bounded operation history and the worker input envelope.
//! These messages are acceptance-owned; their field numbers have no Temporal analog.

use serde::{Deserialize, Serialize};
use tokeira_chasm::DeploymentVersionTarget;

/// The single root proto; creating a resource starts with generation one converged.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct ResourceState {
    /// Original create request, retained across updates for idempotency.
    #[prost(string, tag = "1")]
    pub create_request_id: String,
    /// Original input digest, retained across updates for idempotency.
    #[prost(string, tag = "2")]
    pub create_digest: String,
    /// Latest requested generation.
    #[prost(uint64, tag = "3")]
    pub desired_generation: u64,
    /// Latest successfully reconciled generation.
    #[prost(uint64, tag = "4")]
    pub observed_generation: u64,
    /// At most one external operation may be outstanding.
    #[prost(message, optional, tag = "5")]
    pub active_operation: Option<Operation>,
    /// Oldest-first outcomes, bounded by the transition that appends them.
    #[prost(message, repeated, tag = "6")]
    pub history: Vec<Operation>,
    /// Consecutive failed operations since the last successful reconciliation.
    #[prost(uint32, tag = "7")]
    pub retry_attempt: u32,
    /// Encoded Temporal Failure, empty before failure or after successful recovery.
    #[prost(bytes = "vec", tag = "8")]
    pub last_failure: Vec<u8>,
    /// The exact worker release that owns this resource.
    #[prost(message, required, tag = "9")]
    pub target: DeploymentVersionTarget,
    /// Queue used by every reconciliation of this resource.
    #[prost(string, tag = "10")]
    pub task_queue: String,
    /// Latest update's digest, separate from the immutable create input.
    #[prost(string, tag = "11")]
    pub desired_digest: String,
}

impl ResourceState {
    /// Stable keyword used by both the command view and typed visibility.
    pub fn status(&self) -> &'static str {
        if self.active_operation.is_some() {
            "Reconciling"
        } else if self
            .history
            .last()
            .is_some_and(|operation| operation.outcome() == OperationOutcome::Failed)
        {
            "Failed"
        } else if self.desired_generation == self.observed_generation {
            "Converged"
        } else {
            "Reconciling"
        }
    }

    pub(crate) fn finish(&mut self, operation: Operation) {
        self.history.push(operation);
        if self.history.len() > 8 {
            self.history.drain(..self.history.len() - 8);
        }
    }
}

/// One staging task's operation, retained unchanged after its terminal outcome.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Operation {
    /// Generation captured when the start task was staged.
    #[prost(uint64, tag = "1")]
    pub generation: u64,
    /// Unique business id for this staging, including the component retry attempt.
    #[prost(string, tag = "2")]
    pub activity_id: String,
    /// Pending while active; completed or failed in history.
    #[prost(enumeration = "OperationOutcome", tag = "3")]
    pub outcome: i32,
    /// Encoded Temporal Failure for every unsuccessful terminal outcome.
    #[prost(bytes = "vec", tag = "4")]
    pub failure: Vec<u8>,
    /// CHASM time at staging, not the eventual worker pickup time.
    #[prost(int64, tag = "5")]
    pub started_at_nanos: i64,
    /// CHASM time at outcome application; zero while pending.
    #[prost(int64, tag = "6")]
    pub finished_at_nanos: i64,
}

/// Resource-level outcomes collapse every unsuccessful activity terminal state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, prost::Enumeration)]
#[repr(i32)]
pub enum OperationOutcome {
    /// External work is outstanding.
    Pending = 0,
    /// Reconciliation succeeded.
    Completed = 1,
    /// Reconciliation failed and may be retried by the component.
    Failed = 2,
}

/// Encoded in a single Temporal Payload, inside the start task's Payloads envelope.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct ReconcileInput {
    /// Desired generation captured by this operation.
    #[prost(uint64, tag = "1")]
    pub generation: u64,
    /// Desired input digest captured by this operation.
    #[prost(string, tag = "2")]
    pub digest: String,
}
