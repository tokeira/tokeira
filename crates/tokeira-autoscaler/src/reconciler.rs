//! Desired-vs-current state reconciliation.
//!
//! # Design
//!
//! The reconciler implements a pure function: given a desired state (produced
//! by loops A/B/C) and a current state (read from the platform via the
//! actuator), it emits a list of [`ScalingAction`]s that would converge the
//! two.
//!
//! # Why produce actions instead of executing them?
//!
//! Separating decision from execution provides three benefits:
//! 1. **Testability** — the reconcile function is a pure diff with no I/O,
//!    making it trivial to property-test.
//! 2. **Observability** — the action list can be logged/metriced before
//!    execution, giving operators visibility into what the autoscaler intends.
//! 3. **Batching control** — the caller can decide execution order, rate
//!    limiting, and partial-failure semantics without the reconciler needing
//!    to know about platform retry policies.

use std::collections::BTreeMap;

/// The autoscaler's intent: what the world should look like after this
/// reconciliation cycle. Built incrementally by loops A, B, and C.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DesiredState {
    pub service_counts: BTreeMap<String, u32>,
    pub asg_capacities: BTreeMap<String, u32>,
    pub drain_intents: BTreeMap<String, DrainPhase>,
}

/// The platform's actual state, read via the actuator before reconciliation.
/// Drain phases come from the retirement loop's record of applied platform
/// steps rather than from the platform, which has no notion of the sequence.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CurrentState {
    pub service_counts: BTreeMap<String, u32>,
    pub asg_capacities: BTreeMap<String, u32>,
    pub drain_intents: BTreeMap<String, DrainPhase>,
}

/// Phases of the node retirement state machine, keyed on the controller's
/// node id. Carried across polls by the leader's retirement loop; the
/// sequence and its gates are documented in `loop_c`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainPhase {
    /// The controller accepted the mark and sent the node its drain
    /// directive; the runtime is relinquishing its bundles.
    ControllerDraining,
    /// The platform has been told to stop scheduling new work onto the host
    /// (e.g., ECS DRAINING status).
    EcsDraining,
    /// The controller reported the node safe to terminate and scale-in
    /// protection has been removed.
    ProtectionCleared,
    /// The host has been terminated and its capacity decremented.
    Terminated,
}

/// A single mutation that the reconciler has determined is necessary to
/// converge desired state with current state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalingAction {
    UpdateService { service: String, desired_count: u32 },
    UpdateAsg { asg: String, desired_capacity: u32 },
    AdvanceDrain { node_id: String, phase: DrainPhase },
}

/// Diff desired against current and produce the minimal set of actions needed
/// to converge.
///
/// This is intentionally a pure function with no side effects — it only
/// compares maps and emits differences. The caller is responsible for
/// executing the returned actions via the actuator.
pub fn reconcile(desired: &DesiredState, current: &CurrentState) -> Vec<ScalingAction> {
    let mut actions = Vec::new();
    for (service, desired_count) in &desired.service_counts {
        if current.service_counts.get(service) != Some(desired_count) {
            actions.push(ScalingAction::UpdateService {
                service: service.clone(),
                desired_count: *desired_count,
            });
        }
    }
    for (asg, desired_capacity) in &desired.asg_capacities {
        if current.asg_capacities.get(asg) != Some(desired_capacity) {
            actions.push(ScalingAction::UpdateAsg {
                asg: asg.clone(),
                desired_capacity: *desired_capacity,
            });
        }
    }
    for (node_id, desired_phase) in &desired.drain_intents {
        if current.drain_intents.get(node_id) != Some(desired_phase) {
            actions.push(ScalingAction::AdvanceDrain {
                node_id: node_id.clone(),
                phase: *desired_phase,
            });
        }
    }
    actions
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use proptest::{collection::btree_map, prelude::*};

    use super::*;

    #[test]
    fn matching_desired_and_current_produces_no_actions() {
        let mut desired = DesiredState::default();
        desired.service_counts.insert("edge".into(), 2);
        desired.asg_capacities.insert("runtime".into(), 4);
        desired
            .drain_intents
            .insert("i-123".into(), DrainPhase::EcsDraining);
        let current = CurrentState {
            service_counts: desired.service_counts.clone(),
            asg_capacities: desired.asg_capacities.clone(),
            drain_intents: desired.drain_intents.clone(),
        };

        assert!(reconcile(&desired, &current).is_empty());
    }

    #[test]
    fn differing_desired_and_current_produces_expected_actions() {
        let mut desired = DesiredState::default();
        desired.service_counts.insert("edge".into(), 3);
        desired.asg_capacities.insert("runtime".into(), 5);
        desired
            .drain_intents
            .insert("i-123".into(), DrainPhase::ProtectionCleared);

        let mut current = CurrentState::default();
        current.service_counts.insert("edge".into(), 2);
        current.asg_capacities.insert("runtime".into(), 4);
        current
            .drain_intents
            .insert("i-123".into(), DrainPhase::EcsDraining);

        assert_eq!(
            reconcile(&desired, &current),
            vec![
                ScalingAction::UpdateService {
                    service: "edge".into(),
                    desired_count: 3,
                },
                ScalingAction::UpdateAsg {
                    asg: "runtime".into(),
                    desired_capacity: 5,
                },
                ScalingAction::AdvanceDrain {
                    node_id: "i-123".into(),
                    phase: DrainPhase::ProtectionCleared,
                },
            ]
        );
    }

    proptest! {
        #[test]
        fn property_matching_desired_and_current_is_idempotent(
            service_counts in count_map(),
            asg_capacities in count_map(),
            drain_intents in drain_map(),
        ) {
            let desired = DesiredState {
                service_counts,
                asg_capacities,
                drain_intents,
            };
            let current = CurrentState {
                service_counts: desired.service_counts.clone(),
                asg_capacities: desired.asg_capacities.clone(),
                drain_intents: desired.drain_intents.clone(),
            };

            prop_assert!(reconcile(&desired, &current).is_empty());
        }
    }

    fn count_map() -> impl Strategy<Value = BTreeMap<String, u32>> {
        btree_map("[a-z]{1,12}", 0u32..100, 0..16)
    }

    fn drain_map() -> impl Strategy<Value = BTreeMap<String, DrainPhase>> {
        btree_map("[a-z]{1,12}", drain_phase(), 0..16)
    }

    fn drain_phase() -> impl Strategy<Value = DrainPhase> {
        prop_oneof![
            Just(DrainPhase::ControllerDraining),
            Just(DrainPhase::EcsDraining),
            Just(DrainPhase::ProtectionCleared),
            Just(DrainPhase::Terminated),
        ]
    }
}
