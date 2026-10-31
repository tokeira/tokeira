//! Peer delivery enters the same hydrated destination handler as local delivery.
//! The destination cannot settle source work and never recursively forwards it.

use std::sync::Arc;

use tokeira_storage::{CommitResult, RecoveryError, RunRepository, WriteFence};

use super::TokeiraRuntime;
use crate::{
    DeliveryPhase, KernelRejected, NotShardOwner, OutboundDeliveryOutcome,
    OutboundDeliveryReference, OutboundDeliveryTransport, OutboundTerminalCause,
    outbound::driver::Attempt,
};

#[cfg(test)]
#[path = "outbound_delivery_tests.rs"]
mod tests;

impl<R: RunRepository + 'static> TokeiraRuntime<R> {
    /// Install the bootstrap-validated transport used by the owned recovery driver.
    pub fn set_outbound_delivery_transport(&self, transport: Arc<dyn OutboundDeliveryTransport>) {
        *self
            .outbound_publisher
            .outbound_transport
            .write()
            .expect("outbound transport lock poisoned") = Some(transport);
        self.outbound_publisher.outbound_wake.notify_one();
    }

    /// Apply an authenticated committed reference at its current destination home.
    /// A remote request must carry a durable source lease; single-node unfenced
    /// execution has no peer route and uses the local handler directly.
    pub async fn deliver_outbound_reference(
        &self,
        reference: OutboundDeliveryReference,
    ) -> OutboundDeliveryOutcome {
        if self.ready().await.is_err() || self.runtime_drain.is_draining() {
            return OutboundDeliveryOutcome::NotOwner;
        }
        if !matches!(reference.source_fence, WriteFence::Lease(_)) {
            return OutboundDeliveryOutcome::Obsolete;
        }
        let Some(attempt) = Attempt::capture(&self.shard_owner, reference.destination_home) else {
            return OutboundDeliveryOutcome::NotOwner;
        };
        match self
            .outbound_publisher
            .receive_outbound(&reference, attempt, &self.shard_owner)
            .await
        {
            Ok(CommitResult::Applied { new_state }) => OutboundDeliveryOutcome::Applied {
                run_id: new_state.run_id,
                seq: new_state.transition_seq,
            },
            Ok(_) => OutboundDeliveryOutcome::Retryable,
            Err(error)
                if error.downcast_ref::<NotShardOwner>().is_some()
                    || error
                        .downcast_ref::<tokeira_storage::LeaseFenceLost>()
                        .is_some() =>
            {
                OutboundDeliveryOutcome::NotOwner
            }
            Err(error)
                if matches!(
                    error.downcast_ref::<RecoveryError>(),
                    Some(RecoveryError::Obsolete)
                ) =>
            {
                OutboundDeliveryOutcome::Obsolete
            }
            Err(error) if reference.phase == DeliveryPhase::External => {
                match error.downcast_ref::<KernelRejected>() {
                    Some(KernelRejected(tokeira_kernel::Reject::MissingRun)) => {
                        OutboundDeliveryOutcome::Terminal(OutboundTerminalCause::MissingExecution)
                    }
                    Some(KernelRejected(tokeira_kernel::Reject::RunClosed(_))) => {
                        OutboundDeliveryOutcome::Terminal(OutboundTerminalCause::ClosedExecution)
                    }
                    Some(KernelRejected(tokeira_kernel::Reject::SignalLimitExceeded)) => {
                        OutboundDeliveryOutcome::Terminal(OutboundTerminalCause::SignalLimit)
                    }
                    _ => OutboundDeliveryOutcome::Retryable,
                }
            }
            Err(_) => OutboundDeliveryOutcome::Retryable,
        }
    }
}
