//! Reference-only destination admission shared by local and authenticated peer
//! delivery. Transport observations never retire work or authorize source writes.

use anyhow::Result;
use async_trait::async_trait;
use tokeira_storage::{DeliveryGuard, WriteFence};
use tokeira_types::{RunId, RunKey, ShardId, TransitionSeq};

/// Narrow phase selector; each phase has one persisted source recipe.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeliveryPhase {
    /// Create the run selected by the committed child seed.
    ChildStart,
    /// Apply the accepted TerminateIfRunning policy before child creation.
    ChildReplace,
    /// Release the confirmed child's first task.
    ChildRelease,
    /// Deliver an initiated external signal or cancellation.
    External,
    /// Apply one committed parent-close action.
    ParentClose,
    /// Resolve the exact parent initiation from a terminal child.
    ChildResult,
}

/// Bounded identifiers pointing at committed authority. No workflow payload or
/// caller-authored command may cross the internal delivery endpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboundDeliveryReference {
    /// Exact operation incarnation or immutable close intent.
    pub authority: DeliveryGuard,
    /// Original source lease, rechecked inside the destination transaction.
    pub source_fence: WriteFence,
    /// Phase must agree with the stored operation/intent kind.
    pub phase: DeliveryPhase,
    /// Selected destination; hydration verifies its identity and relationship.
    pub destination: RunKey,
    /// Placement hint, checked against the hydrated identity.
    pub destination_home: ShardId,
}

/// Definitive external-operation refusals; transport failures are never causes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutboundTerminalCause {
    /// The selected execution does not exist.
    MissingExecution,
    /// A signal selected a closed execution.
    ClosedExecution,
    /// The target has reached its accepted signal limit.
    SignalLimit,
}

/// Small peer response, verified by the source before resolving its obligation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutboundDeliveryOutcome {
    /// Destination transaction committed against the supplied source authority.
    Applied {
        /// Execution identity, checked against the selected destination.
        run_id: RunId,
        /// Committed destination sequence, checked against reloaded state.
        seq: TransitionSeq,
    },
    /// A typed, phase-specific destination refusal.
    Terminal(OutboundTerminalCause),
    /// The committed reference no longer describes pending work.
    Obsolete,
    /// Placement or acquisition changed; rediscover before retrying.
    NotOwner,
    /// No definitive outcome; the durable source obligation remains pending.
    Retryable,
}

/// Bounded transport installed only by peer-enabled bootstrap. Implementations
/// route from current placement and never forward receiver-supplied URLs.
#[async_trait]
pub trait OutboundDeliveryTransport: Send + Sync + std::fmt::Debug {
    /// Perform one bounded owner attempt; discovery supplies further retries.
    async fn deliver(
        &self,
        reference: OutboundDeliveryReference,
    ) -> Result<OutboundDeliveryOutcome>;
}
