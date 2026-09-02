//! Loop C: runtime retirement gated on the controller's drain verdict.
//!
//! # Why is retirement a multi-step process?
//!
//! Terminating a runtime host immediately would kill in-flight workflow tasks,
//! causing visible failures and requiring replay recovery. Instead, retirement
//! follows the safe scale-in protocol of architecture note 045: Mimir decides
//! *whether* the fleet has excess capacity, the controller decides *which*
//! node leaves, and the platform steps happen only as the controller confirms
//! progress:
//!
//! 1. **ControllerDraining** — the controller accepted the mark and sent the
//!    node its drain directive; the runtime relinquishes its bundles.
//! 2. **EcsDraining** — the platform is told to stop scheduling new tasks on
//!    the host (ECS DRAINING, Kubernetes cordon). Follows the mark directly.
//! 3. **ProtectionCleared** — scale-in protection is removed. Entered only
//!    when the controller reports the node's own heartbeat verdict
//!    `SAFE_TO_TERMINATE`: no owned bundles, no in-flight transitions, no
//!    outstanding workflow-task replies.
//! 4. **Terminated** — the host is terminated with an atomic capacity
//!    decrement so no replacement is launched.
//!
//! The controller marks a node draining exactly once per retirement. Progress
//! is read through `DescribeNodeDrain`, never by re-marking, because a repeat
//! mark is a new drain request rather than a query.
//!
//! # State across polls
//!
//! [`RetirementLoop`] carries every open retirement's desired and applied
//! phase from one poll to the next, so a phase is entered once and the
//! reconciler emits each platform step exactly once. The state lives in the
//! leader process: a restart forgets open retirements, leaving the controller
//! holding the node draining with its bundles already moved. Recovering that
//! after a restart is a stated non-goal of this loop today.
//!
//! # Identity
//!
//! Retirements are keyed on the controller's node id (the runtime
//! incarnation). The actuator resolves platform identity from it at each
//! phase; how a node id maps to an EC2 instance is the platform actuator's
//! concern.
//!
//! # Why separate from Loop B?
//!
//! Scale-out (Loop B) and retirement (Loop C) operate on different time
//! scales. Scale-out is fast (seconds) because under-provisioning causes
//! immediate user-visible latency. Retirement is slow (minutes) because it
//! must wait for workload migration. Coupling them would either make scale-out
//! too slow or retirement too aggressive.

use std::collections::BTreeMap;

use anyhow::Result;
use tokeira_observability::{AutoscalerLoopLabel, NominationOutcomeLabel, ScalingDirectionLabel};
use tokeira_proto::connect::tokeira::internal::controller::v1::NodeDrainState;
use tracing::warn;

use crate::{
    actuator::Actuator, controller_client::PlacementControl, metrics, reconciler::DrainPhase,
};

/// A node the controller nominated for retirement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetirementCandidate {
    pub(crate) node_id: String,
}

/// Retirement state carried across polls by the leader.
#[derive(Debug, Default)]
pub struct RetirementLoop {
    /// Phase each retiring node should reach next; becomes
    /// `DesiredState::drain_intents`.
    desired: BTreeMap<String, DrainPhase>,
    /// Phase the platform has confirmed for each node; becomes
    /// `CurrentState::drain_intents`.
    applied: BTreeMap<String, DrainPhase>,
}

impl RetirementLoop {
    /// Observe the controller and decide this cycle's retirement phases.
    ///
    /// Open retirements advance only on confirmation: `EcsDraining` leaves for
    /// `ProtectionCleared` when the controller reports `SAFE_TO_TERMINATE`, and
    /// each later phase waits for the previous platform step to have been
    /// applied. When `excess_capacity` holds and no retirement is open, one
    /// node is nominated and marked; only an accepted mark opens a retirement.
    /// One retirement at a time keeps every drain observable end to end and
    /// bounds the capacity removed per cycle to a single host.
    pub async fn plan(
        &mut self,
        controller: &dyn PlacementControl,
        excess_capacity: bool,
    ) -> Result<()> {
        for (node_id, desired) in self.desired.clone() {
            let applied = self.applied.get(&node_id).copied();
            match (desired, applied) {
                (DrainPhase::EcsDraining, Some(DrainPhase::EcsDraining)) => {
                    match controller.describe_node_drain(&node_id).await? {
                        Some(NodeDrainState::NODE_DRAIN_STATE_SAFE_TO_TERMINATE) => {
                            self.desired.insert(node_id, DrainPhase::ProtectionCleared);
                            metrics::record_scaling_decision(
                                AutoscalerLoopLabel::Retirement,
                                ScalingDirectionLabel::Down,
                                "drain_complete",
                            );
                        }
                        Some(_) => metrics::record_scaling_decision(
                            AutoscalerLoopLabel::Retirement,
                            ScalingDirectionLabel::Hold,
                            "draining",
                        ),
                        None => {
                            // The controller lost the node (its incarnation
                            // is gone) before it reported safe. Its bundles
                            // are protected by lease expiry and epoch fencing,
                            // but nothing can confirm the host is idle, so the
                            // retirement holds for an operator.
                            warn!(
                                node_id,
                                "retirement held: controller has no drain record for the node"
                            );
                            metrics::record_scaling_decision(
                                AutoscalerLoopLabel::Retirement,
                                ScalingDirectionLabel::Hold,
                                "node_unknown",
                            );
                        }
                    }
                }
                (DrainPhase::ProtectionCleared, Some(DrainPhase::ProtectionCleared)) => {
                    self.desired.insert(node_id, DrainPhase::Terminated);
                }
                (DrainPhase::Terminated, Some(DrainPhase::Terminated)) => {
                    self.desired.remove(&node_id);
                    self.applied.remove(&node_id);
                }
                // The platform step for the current phase has not been
                // applied yet; the reconciler re-emits it this cycle.
                _ => {}
            }
        }

        if !self.desired.is_empty() {
            return Ok(());
        }
        if !excess_capacity {
            metrics::record_scaling_decision(
                AutoscalerLoopLabel::Retirement,
                ScalingDirectionLabel::Hold,
                "no_excess",
            );
            return Ok(());
        }
        let nomination = controller.nominate_scale_in_candidates(1).await?;
        let Some(candidate) = nomination.candidates.into_iter().next() else {
            metrics::record_nomination(NominationOutcomeLabel::Rejected);
            metrics::record_scaling_decision(
                AutoscalerLoopLabel::Retirement,
                ScalingDirectionLabel::Hold,
                "no_candidate",
            );
            return Ok(());
        };
        if !controller.mark_node_draining(&candidate.node_id).await? {
            metrics::record_nomination(NominationOutcomeLabel::Rejected);
            metrics::record_scaling_decision(
                AutoscalerLoopLabel::Retirement,
                ScalingDirectionLabel::Hold,
                "mark_rejected",
            );
            return Ok(());
        }
        metrics::record_nomination(NominationOutcomeLabel::Accepted);
        metrics::record_scaling_decision(
            AutoscalerLoopLabel::Retirement,
            ScalingDirectionLabel::Down,
            "retirement_candidate",
        );
        // The accepted mark is the controller phase already applied; the
        // platform's first step follows it directly (045, step 5).
        self.applied
            .insert(candidate.node_id.clone(), DrainPhase::ControllerDraining);
        self.desired
            .insert(candidate.node_id, DrainPhase::EcsDraining);
        Ok(())
    }

    /// Phases each retiring node should reach next.
    pub fn desired_intents(&self) -> BTreeMap<String, DrainPhase> {
        self.desired.clone()
    }

    /// Phases the platform has confirmed.
    pub fn applied_intents(&self) -> BTreeMap<String, DrainPhase> {
        self.applied.clone()
    }

    /// Record that the platform step for `phase` succeeded.
    pub fn record_applied(&mut self, node_id: &str, phase: DrainPhase) {
        self.applied.insert(node_id.to_owned(), phase);
    }
}

/// Perform the platform step that enters `phase` for one retiring node.
///
/// `ControllerDraining` is the controller's own mark and needs no platform
/// action. The remaining phases follow 045 steps 5, 7, and 8.
pub async fn apply_drain_phase(
    actuator: &dyn Actuator,
    cluster: &str,
    asg_name: &str,
    node_id: &str,
    phase: DrainPhase,
) -> Result<()> {
    match phase {
        DrainPhase::ControllerDraining => Ok(()),
        DrainPhase::EcsDraining => {
            let container_instance = actuator
                .resolve_container_instance_for_ec2(cluster, node_id)
                .await?;
            actuator
                .drain_container_instance(cluster, &container_instance)
                .await
        }
        DrainPhase::ProtectionCleared => {
            actuator.clear_instance_protection(asg_name, node_id).await
        }
        DrainPhase::Terminated => actuator.terminate_instance_with_decrement(node_id).await,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use anyhow::bail;
    use async_trait::async_trait;

    use super::*;
    use crate::{
        actuator::test_support::MockActuator,
        controller_client::NominationResult,
        reconciler::{CurrentState, DesiredState, ScalingAction, reconcile},
    };

    #[derive(Debug)]
    struct ScriptedController {
        candidate: Option<String>,
        accept_mark: bool,
        drain_state: Mutex<Option<NodeDrainState>>,
        calls: Mutex<Vec<String>>,
    }

    impl ScriptedController {
        fn new(candidate: Option<&str>, accept_mark: bool) -> Self {
            Self {
                candidate: candidate.map(str::to_owned),
                accept_mark,
                drain_state: Mutex::new(Some(NodeDrainState::NODE_DRAIN_STATE_DRAINING)),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn report(&self, state: Option<NodeDrainState>) {
            *self.drain_state.lock().expect("drain_state lock") = state;
        }

        fn calls(&self) -> Vec<String> {
            self.calls.lock().expect("calls lock").clone()
        }
    }

    #[async_trait]
    impl PlacementControl for ScriptedController {
        async fn nominate_scale_in_candidates(&self, limit: u32) -> Result<NominationResult> {
            self.calls
                .lock()
                .expect("calls lock")
                .push(format!("nominate:{limit}"));
            Ok(NominationResult {
                candidates: self
                    .candidate
                    .iter()
                    .map(|node_id| RetirementCandidate {
                        node_id: node_id.clone(),
                    })
                    .collect(),
                aggregate_available_connections: 0,
                aggregate_connection_rate_headroom: 0.0,
            })
        }

        async fn mark_node_draining(&self, node_id: &str) -> Result<bool> {
            self.calls
                .lock()
                .expect("calls lock")
                .push(format!("mark:{node_id}"));
            Ok(self.accept_mark)
        }

        async fn describe_node_drain(&self, node_id: &str) -> Result<Option<NodeDrainState>> {
            self.calls
                .lock()
                .expect("calls lock")
                .push(format!("describe:{node_id}"));
            match *self.drain_state.lock().expect("drain_state lock") {
                Some(NodeDrainState::NODE_DRAIN_STATE_UNSPECIFIED) => bail!("scripted failure"),
                state => Ok(state),
            }
        }
    }

    /// One leader poll: plan, reconcile, apply each drain step through the
    /// actuator, record what succeeded.
    async fn poll(
        retirements: &mut RetirementLoop,
        controller: &ScriptedController,
        actuator: &MockActuator,
        excess_capacity: bool,
    ) -> Result<Vec<ScalingAction>> {
        retirements.plan(controller, excess_capacity).await?;
        let desired = DesiredState {
            drain_intents: retirements.desired_intents(),
            ..DesiredState::default()
        };
        let current = CurrentState {
            drain_intents: retirements.applied_intents(),
            ..CurrentState::default()
        };
        let actions = reconcile(&desired, &current);
        for action in &actions {
            if let ScalingAction::AdvanceDrain { node_id, phase } = action {
                apply_drain_phase(actuator, "cluster", "runtime-asg", node_id, *phase).await?;
                retirements.record_applied(node_id, *phase);
            }
        }
        Ok(actions)
    }

    #[tokio::test]
    async fn rejected_mark_opens_no_retirement() {
        let controller = ScriptedController::new(Some("node-a"), false);
        let actuator = MockActuator::default();
        let mut retirements = RetirementLoop::default();

        let actions = poll(&mut retirements, &controller, &actuator, true)
            .await
            .unwrap();

        assert!(actions.is_empty());
        assert!(retirements.desired_intents().is_empty());
        assert_eq!(controller.calls(), vec!["nominate:1", "mark:node-a"]);
        assert!(actuator.calls().is_empty());
    }

    #[tokio::test]
    async fn no_excess_capacity_asks_the_controller_for_nothing() {
        let controller = ScriptedController::new(Some("node-a"), true);
        let actuator = MockActuator::default();
        let mut retirements = RetirementLoop::default();

        poll(&mut retirements, &controller, &actuator, false)
            .await
            .unwrap();

        assert!(controller.calls().is_empty());
        assert!(retirements.desired_intents().is_empty());
    }

    #[tokio::test]
    async fn retirement_holds_in_ecs_draining_until_the_controller_reports_safe() {
        let controller = ScriptedController::new(Some("node-a"), true);
        let actuator = MockActuator::default();
        let mut retirements = RetirementLoop::default();

        // Accepted mark: the platform drain follows the controller mark.
        poll(&mut retirements, &controller, &actuator, true)
            .await
            .unwrap();
        assert_eq!(
            retirements.applied_intents()["node-a"],
            DrainPhase::EcsDraining
        );
        assert_eq!(
            actuator.calls(),
            vec![
                "resolve:cluster:node-a".to_owned(),
                "drain:cluster:container:node-a".to_owned()
            ]
        );

        // Still draining, and a cycle where the controller is unreachable:
        // nothing on the platform moves.
        for _ in 0..2 {
            poll(&mut retirements, &controller, &actuator, true)
                .await
                .unwrap();
        }
        controller.report(Some(NodeDrainState::NODE_DRAIN_STATE_UNSPECIFIED));
        assert!(
            poll(&mut retirements, &controller, &actuator, true)
                .await
                .is_err()
        );
        controller.report(None);
        poll(&mut retirements, &controller, &actuator, true)
            .await
            .unwrap();
        assert_eq!(
            retirements.desired_intents()["node-a"],
            DrainPhase::EcsDraining
        );
        assert_eq!(actuator.calls().len(), 2);
        // No second retirement opens while one is in flight.
        assert_eq!(
            controller
                .calls()
                .iter()
                .filter(|call| call.starts_with("nominate"))
                .count(),
            1
        );

        // The runtime's own verdict opens the rest of the sequence.
        controller.report(Some(NodeDrainState::NODE_DRAIN_STATE_SAFE_TO_TERMINATE));
        poll(&mut retirements, &controller, &actuator, true)
            .await
            .unwrap();
        assert_eq!(
            retirements.applied_intents()["node-a"],
            DrainPhase::ProtectionCleared
        );
        poll(&mut retirements, &controller, &actuator, true)
            .await
            .unwrap();
        assert_eq!(
            retirements.applied_intents()["node-a"],
            DrainPhase::Terminated
        );
        assert_eq!(
            actuator.calls(),
            vec![
                "resolve:cluster:node-a".to_owned(),
                "drain:cluster:container:node-a".to_owned(),
                "clear_protection:runtime-asg:node-a".to_owned(),
                "terminate:node-a".to_owned(),
            ]
        );

        // The finished retirement is forgotten, and excess capacity opens the next one.
        poll(&mut retirements, &controller, &actuator, true)
            .await
            .unwrap();
        assert!(retirements.desired_intents().contains_key("node-a"));
        assert_eq!(
            retirements.applied_intents()["node-a"],
            DrainPhase::EcsDraining
        );
        assert_eq!(
            controller
                .calls()
                .iter()
                .filter(|call| call.starts_with("nominate"))
                .count(),
            2
        );
    }
}
