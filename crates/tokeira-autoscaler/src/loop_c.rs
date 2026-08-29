//! Loop C: Runtime instance retirement via a multi-phase drain state machine.
//!
//! # Why is retirement a multi-step process?
//!
//! Terminating a runtime host immediately would kill in-flight workflow tasks,
//! causing visible failures and requiring replay recovery. Instead, retirement
//! proceeds through a state machine:
//!
//! 1. **ControllerDraining** — The autoscaler's controller stops assigning new
//!    bundles to this host and begins migrating existing bundles away.
//! 2. **EcsDraining** — The platform is told to stop scheduling new tasks
//!    (ECS DRAINING, Kubernetes cordon). Existing tasks run to completion.
//! 3. **ProtectionCleared** — Scale-in protection is removed. The host is now
//!    eligible for termination but hasn't been terminated yet.
//! 4. **Terminated** — The host is terminated with an atomic capacity
//!    decrement so no replacement is launched.
//!
//! Each phase transition is persisted in the desired state so that if the
//! autoscaler crashes mid-retirement, it resumes from the correct phase
//! rather than re-draining an already-drained host or terminating one that
//! still has active work.
//!
//! # Why separate from Loop B?
//!
//! Scale-out (Loop B) and retirement (Loop C) operate on different time
//! scales. Scale-out is fast (seconds) because under-provisioning causes
//! immediate user-visible latency. Retirement is slow (minutes) because it
//! must wait for workload migration. Coupling them would either make scale-out
//! too slow or retirement too aggressive.

use crate::reconciler::{DesiredState, DrainPhase};

/// A host that has been selected for retirement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetirementCandidate {
    pub(crate) instance_id: String,
}

/// Begin the retirement process for a candidate instance.
///
/// Returns `false` if the instance is already being retired (idempotent
/// guard against duplicate retirement requests from consecutive poll cycles).
pub fn request_runtime_retirement(
    desired: &mut DesiredState,
    candidate: RetirementCandidate,
) -> bool {
    if desired.drain_intents.contains_key(&candidate.instance_id) {
        return false;
    }
    desired
        .drain_intents
        .insert(candidate.instance_id, DrainPhase::ControllerDraining);
    true
}

/// Advance a retiring instance to the next phase of the drain state machine.
///
/// Returns the new phase, or `None` if the instance isn't being retired.
/// The `Terminated` phase is a terminal state — advancing from it is a no-op.
pub fn advance_drain_phase(desired: &mut DesiredState, instance_id: &str) -> Option<DrainPhase> {
    let current = desired.drain_intents.get(instance_id).copied()?;
    let next = match current {
        DrainPhase::ControllerDraining => DrainPhase::EcsDraining,
        DrainPhase::EcsDraining => DrainPhase::ProtectionCleared,
        DrainPhase::ProtectionCleared => DrainPhase::Terminated,
        DrainPhase::Terminated => DrainPhase::Terminated,
    };
    desired.drain_intents.insert(instance_id.to_owned(), next);
    Some(next)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retirement_request_is_idempotent() {
        let mut desired = DesiredState::default();
        let candidate = RetirementCandidate {
            instance_id: "i-abc".to_owned(),
        };

        assert!(request_runtime_retirement(&mut desired, candidate.clone()));
        assert!(!request_runtime_retirement(&mut desired, candidate));
        assert_eq!(
            desired.drain_intents["i-abc"],
            DrainPhase::ControllerDraining
        );
    }

    #[test]
    fn drain_phase_progresses_to_terminal_state() {
        let mut desired = DesiredState::default();
        desired
            .drain_intents
            .insert("i-abc".to_owned(), DrainPhase::ControllerDraining);

        assert_eq!(
            advance_drain_phase(&mut desired, "i-abc"),
            Some(DrainPhase::EcsDraining)
        );
        assert_eq!(
            advance_drain_phase(&mut desired, "i-abc"),
            Some(DrainPhase::ProtectionCleared)
        );
        assert_eq!(
            advance_drain_phase(&mut desired, "i-abc"),
            Some(DrainPhase::Terminated)
        );
        assert_eq!(
            advance_drain_phase(&mut desired, "i-abc"),
            Some(DrainPhase::Terminated)
        );
    }
}
