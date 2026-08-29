//! Placement-controller metric definitions and bounded recording helpers.
//!
//! Controller labels describe controller outcomes and aggregate sizes only.
//! Runtime node IDs stay in logs/spans because they are instance-cardinality
//! values and would make controller metrics expensive in multi-node clusters.

use metrics::{counter, gauge, histogram};
use tokeira_observability::{ControllerCasOutcomeLabel, OutcomeLabel};
use tokeira_types::MetricType;
#[cfg(test)]
use tokeira_types::validate_metric_name;

pub(crate) const PLACEMENT_LOOP_DURATION_SECONDS: &str =
    "tokeira_controller_placement_loop_duration_seconds";
pub(crate) const GENERATION_CAS_TOTAL: &str = "tokeira_controller_generation_cas_total";
pub(crate) const ROUTING_SNAPSHOT_SIZE: &str = "tokeira_controller_routing_snapshot_size";
pub(crate) const BUNDLE_OWNERSHIP_CHURN_TOTAL: &str =
    "tokeira_controller_bundle_ownership_churn_total";
pub(crate) const DRAIN_ACTIVE_NODES: &str = "tokeira_controller_drain_active_nodes";
pub(crate) const BUDGET_ALLOCATION_TOTAL: &str = "tokeira_controller_budget_allocation_total";
pub(crate) const MEMBERSHIP_NODES_TOTAL: &str = "tokeira_controller_membership_nodes_total";

pub const METRIC_NAMES: &[(&str, MetricType)] = &[
    (
        PLACEMENT_LOOP_DURATION_SECONDS,
        MetricType::DurationHistogram,
    ),
    (GENERATION_CAS_TOTAL, MetricType::Counter),
    (ROUTING_SNAPSHOT_SIZE, MetricType::Gauge),
    (BUNDLE_OWNERSHIP_CHURN_TOTAL, MetricType::Counter),
    (DRAIN_ACTIVE_NODES, MetricType::Gauge),
    (BUDGET_ALLOCATION_TOTAL, MetricType::Counter),
    (MEMBERSHIP_NODES_TOTAL, MetricType::Gauge),
];

pub fn record_placement_loop_duration(duration: std::time::Duration) {
    histogram!(PLACEMENT_LOOP_DURATION_SECONDS).record(duration.as_secs_f64());
}

pub(crate) fn record_generation_cas(outcome: ControllerCasOutcomeLabel) {
    counter!(GENERATION_CAS_TOTAL, "outcome" => outcome.as_str()).increment(1);
}

pub(crate) fn set_routing_snapshot_size(size: usize) {
    gauge!(ROUTING_SNAPSHOT_SIZE).set(size as f64);
}

pub(crate) fn record_bundle_ownership_churn(count: usize) {
    if count > 0 {
        counter!(BUNDLE_OWNERSHIP_CHURN_TOTAL).increment(count as u64);
    }
}

pub(crate) fn set_drain_active_nodes(count: usize) {
    gauge!(DRAIN_ACTIVE_NODES).set(count as f64);
}

pub(crate) fn record_budget_allocation(outcome: OutcomeLabel) {
    counter!(BUDGET_ALLOCATION_TOTAL, "outcome" => outcome.as_str()).increment(1);
}

pub(crate) fn set_membership_nodes_total(count: usize) {
    gauge!(MEMBERSHIP_NODES_TOTAL).set(count as f64);
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use metrics::with_local_recorder;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    fn snapshot_map(
        recorder: &DebuggingRecorder,
    ) -> HashMap<String, (HashMap<String, String>, DebugValue)> {
        recorder
            .snapshotter()
            .snapshot()
            .into_vec()
            .into_iter()
            .map(|(key, _, _, value)| {
                let labels = key
                    .key()
                    .labels()
                    .map(|label| (label.key().to_string(), label.value().to_string()))
                    .collect::<HashMap<_, _>>();
                (key.key().name().to_string(), (labels, value))
            })
            .collect()
    }

    #[test]
    fn manifest_uses_valid_metric_names() {
        for (name, kind) in METRIC_NAMES {
            validate_metric_name(name, *kind).unwrap();
        }
    }

    #[test]
    fn helpers_emit_expected_metrics_and_labels() {
        let recorder = DebuggingRecorder::new();

        with_local_recorder(&recorder, || {
            record_placement_loop_duration(std::time::Duration::from_millis(23));
            record_generation_cas(ControllerCasOutcomeLabel::Success);
            set_routing_snapshot_size(32);
            record_bundle_ownership_churn(4);
            set_drain_active_nodes(2);
            record_budget_allocation(OutcomeLabel::Conflict);
            set_membership_nodes_total(5);
        });

        let snapshot = snapshot_map(&recorder);
        assert!(snapshot.contains_key(PLACEMENT_LOOP_DURATION_SECONDS));

        let (labels, value) = snapshot.get(GENERATION_CAS_TOTAL).unwrap();
        assert_eq!(labels.get("outcome"), Some(&"success".to_string()));
        assert_eq!(value, &DebugValue::Counter(1));

        assert_eq!(
            snapshot.get(ROUTING_SNAPSHOT_SIZE).unwrap().1,
            DebugValue::Gauge(32.0.into())
        );
        assert_eq!(
            snapshot.get(BUNDLE_OWNERSHIP_CHURN_TOTAL).unwrap().1,
            DebugValue::Counter(4)
        );
        assert_eq!(
            snapshot.get(DRAIN_ACTIVE_NODES).unwrap().1,
            DebugValue::Gauge(2.0.into())
        );

        let (labels, value) = snapshot.get(BUDGET_ALLOCATION_TOTAL).unwrap();
        assert_eq!(labels.get("outcome"), Some(&"conflict".to_string()));
        assert_eq!(value, &DebugValue::Counter(1));

        assert_eq!(
            snapshot.get(MEMBERSHIP_NODES_TOTAL).unwrap().1,
            DebugValue::Gauge(5.0.into())
        );
    }
}
