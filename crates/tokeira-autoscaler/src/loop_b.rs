//! Loop B: Runtime scale-out gated by DSQL headroom.
//!
//! # Why only BroadSaturation triggers scale-out?
//!
//! The autoscaler distinguishes several pressure signals (hot-node imbalance,
//! hot-bundle imbalance, admission bounds, etc.), but only `BroadSaturation`
//! — meaning ALL runtime hosts are under pressure — justifies adding capacity.
//! Other signals indicate placement or routing problems that adding hosts
//! won't solve and may even worsen (e.g., a hot bundle will remain hot
//! regardless of fleet size).
//!
//! # Why is DSQL headroom a gate?
//!
//! Each new runtime host opens a reserved set of DSQL connections at startup.
//! If the cluster is already near its DSQL connection budget, adding a host
//! would push it over the limit, causing connection failures across the entire
//! fleet. The `dsql_headroom_available` flag (derived from the scaling
//! envelope) prevents scale-out when the connection budget cannot absorb
//! another host's reservation.

use crate::{envelope::ScalingEnvelope, reconciler::DesiredState};

/// Classifies the type of runtime pressure observed across the fleet.
///
/// Only `BroadSaturation` triggers scale-out. The other variants are
/// informational — they explain WHY the fleet is under pressure but indicate
/// problems that horizontal scaling alone cannot solve.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimePressure {
    /// All hosts are saturated — the fleet genuinely needs more capacity.
    BroadSaturation,
    /// A single node is hot due to uneven shard placement.
    HotNodeImbalance,
    /// A single bundle is hot due to a large workflow or activity burst.
    HotBundleImbalance,
    /// DSQL connection budget is exhausted — cannot add more hosts.
    DsqlBound,
    /// Admission control is rejecting work — a backpressure signal, not a
    /// capacity signal.
    AdmissionBound,
    /// No pressure detected.
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeScaleOutInput {
    pub current_hosts: u32,
    pub step: u32,
    pub pressure: RuntimePressure,
    /// Whether the DSQL connection budget can absorb another `step` hosts.
    pub dsql_headroom_available: bool,
}

/// Attempt to scale out the runtime fleet by `step` hosts.
///
/// Returns `true` if the desired state was mutated (scale-out approved),
/// `false` if any gate condition blocked it.
pub fn apply_runtime_scale_out(
    desired: &mut DesiredState,
    asg_name: &str,
    envelope: ScalingEnvelope,
    input: RuntimeScaleOutInput,
) -> bool {
    if input.pressure != RuntimePressure::BroadSaturation || !input.dsql_headroom_available {
        return false;
    }
    let target = input.current_hosts.saturating_add(input.step.max(1));
    if !envelope.allows_scale_to(target) {
        return false;
    }
    desired.asg_capacities.insert(asg_name.to_owned(), target);
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope() -> ScalingEnvelope {
        ScalingEnvelope {
            configured_max_runtime_hosts: 10,
            dsql_connection_budget: 1_000,
            dsql_connection_rate_budget: 100,
            per_runtime_reserved_connections: 64,
            per_runtime_startup_connection_rate: 10,
        }
    }

    #[test]
    fn scale_out_is_blocked_without_dsql_headroom() {
        let mut desired = DesiredState::default();
        let changed = apply_runtime_scale_out(
            &mut desired,
            "runtime",
            envelope(),
            RuntimeScaleOutInput {
                current_hosts: 3,
                step: 1,
                pressure: RuntimePressure::BroadSaturation,
                dsql_headroom_available: false,
            },
        );

        assert!(!changed);
        assert!(desired.asg_capacities.is_empty());
    }

    #[test]
    fn hot_bundle_imbalance_does_not_scale_runtime_hosts() {
        let mut desired = DesiredState::default();
        let changed = apply_runtime_scale_out(
            &mut desired,
            "runtime",
            envelope(),
            RuntimeScaleOutInput {
                current_hosts: 3,
                step: 1,
                pressure: RuntimePressure::HotBundleImbalance,
                dsql_headroom_available: true,
            },
        );

        assert!(!changed);
        assert!(desired.asg_capacities.is_empty());
    }

    #[test]
    fn broad_saturation_with_headroom_sets_target_capacity() {
        let mut desired = DesiredState::default();
        let changed = apply_runtime_scale_out(
            &mut desired,
            "runtime",
            envelope(),
            RuntimeScaleOutInput {
                current_hosts: 3,
                step: 2,
                pressure: RuntimePressure::BroadSaturation,
                dsql_headroom_available: true,
            },
        );

        assert!(changed);
        assert_eq!(desired.asg_capacities["runtime"], 5);
    }
}
