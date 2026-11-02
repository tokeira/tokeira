//! Queue-home requests and execution-owner decisions for activity starts.
//!
//! One request identity survives transport and OCC retries. A recovered reply
//! observes its original committed start; it never assigns that start to another
//! poll. Protocol projection and exact encoded-size validation remain at the edge.

use std::{fmt::Debug, future::Future, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use tokeira_storage::DispatchableActivityTask;
use tokeira_types::{ShardId, TaskKind, WorkerIdentity};
use tokio::time::Instant;
use uuid::Uuid;

use crate::StartedActivityTask;

/// An admitted match, including its identity across ambiguous owner replies.
#[derive(Clone, Debug)]
pub struct ActivityTaskStartRequest {
    /// Exact incarnation and selected worker target; contains no user payload.
    pub offered: DispatchableActivityTask,
    /// Allocated when the poll takes its offer, never on a transport retry.
    pub request_id: Uuid,
    /// Original admitted worker identity, including a legitimate empty identity.
    pub worker: WorkerIdentity,
}

impl ActivityTaskStartRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        let task = &self.offered;
        if self.request_id.is_nil()
            || task.run_key.0.is_nil()
            || task.queue.namespace_id.0.is_nil()
            || task.queue.task_kind != TaskKind::Activity
            || task.queue.task_queue.0.is_empty()
            || task.activity_id.is_empty()
            || task.schedule_event_id <= 0
            || task.attempt == 0
            || i32::try_from(task.attempt).is_err()
            || i64::try_from(task.stamp).is_err()
            || task.dispatch_revision < 0
        {
            return Err(InvalidActivityStart.into());
        }
        Ok(())
    }
}

/// The execution owner's authoritative disposition of an activity match.
#[derive(Clone, Debug)]
pub enum ActivityStartOutcome {
    /// This call committed the start and its receipt together.
    Started(StartedActivityTask),
    /// This request already committed; the current lease validated its observation.
    Replayed(StartedActivityTask),
    /// A pause, eligibility restriction or deployment transition withheld delivery.
    Suppressed,
    /// The requested incarnation or request identity no longer matches authority.
    Stale,
}

impl ActivityStartOutcome {
    pub(crate) fn into_task(self) -> Option<StartedActivityTask> {
        match self {
            Self::Started(task) | Self::Replayed(task) => Some(task),
            Self::Suppressed | Self::Stale => None,
        }
    }
}

/// Placement-aware transport preserving the same request across bounded attempts.
#[async_trait]
pub trait ActivityTaskStarter: Debug + Send + Sync {
    /// Attempt a start within the original poll deadline; never mint a new request
    /// ID after an ambiguous reply and never recursively forward at the receiver.
    async fn start(
        &self,
        home: ShardId,
        request: ActivityTaskStartRequest,
        deadline: Instant,
    ) -> Result<ActivityStartOutcome>;
}

/// Per-call edge projection check, performed before a new start commits.
///
/// RPC adapters must install this around ordinary, eager and forwarded starts.
/// The check uses the exact native protobuf response, token, origin and envelope;
/// it also checks the final authorization target. It must not mutate run state.
/// Domain-only embedded callers produce no encoded response and need no adapter.
pub trait ActivityStartResponsePreflight: Debug + Send + Sync {
    /// Refuse an unrepresentable, oversized or unauthorized prospective response.
    fn validate(&self, task: &StartedActivityTask) -> Result<()>;
}

tokio::task_local! {
    static RESPONSE_PREFLIGHT: Arc<dyn ActivityStartResponsePreflight>;
}

/// Bind one admitted RPC's projection policy to all activity starts it performs.
/// Credentials remain call-scoped and are never stored in offers or receipts.
pub async fn scope_activity_start_response<F: Future>(
    preflight: Arc<dyn ActivityStartResponsePreflight>,
    future: F,
) -> F::Output {
    RESPONSE_PREFLIGHT.scope(preflight, future).await
}

pub(crate) fn preflight_activity_start(task: &StartedActivityTask) -> Result<()> {
    i32::try_from(task.attempt).map_err(|_| InvalidActivityStart)?;
    RESPONSE_PREFLIGHT
        .try_with(|preflight| preflight.validate(task))
        .unwrap_or(Ok(()))
}

/// A request cannot identify a valid stored activity incarnation.
#[derive(Debug, thiserror::Error)]
#[error("invalid activity start request identity or coordinates")]
pub struct InvalidActivityStart;

/// A projected success cannot fit the reviewed owner-start envelope.
#[derive(Debug, thiserror::Error)]
#[error("activity start response exceeds the 128 MiB encoded envelope")]
pub struct ActivityStartTooLarge;

/// A transient owner or transport failure, recoverable within the same poll.
#[derive(Debug, thiserror::Error)]
#[error("activity execution owner is temporarily unavailable: {0}")]
pub struct RetryableActivityTaskStart(pub String);
