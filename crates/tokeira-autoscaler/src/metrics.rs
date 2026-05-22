//! Autoscaler metric definitions and bounded recording helpers.
//!
//! The autoscaler consumes high-cardinality infrastructure state but exports
//! only loop, direction, reason, and configuration-bounded service labels. Raw
//! instance IDs stay in logs/spans and controller RPC payloads.

use metrics::{counter, gauge, histogram};
use tokeira_observability::{
    AutoscalerLoopLabel, NominationOutcomeLabel, OutcomeLabel, ScalingDirectionLabel,
};
use tokeira_types::MetricType;
#[cfg(test)]
use tokeira_types::validate_metric_name;

pub const LOOP_DURATION_SECONDS: &str = "tokeira_autoscaler_loop_duration_seconds";
pub const SCALING_DECISIONS_TOTAL: &str = "tokeira_autoscaler_scaling_decisions_total";
pub const METRIC_FRESHNESS_AGE_SECONDS: &str = "tokeira_autoscaler_metric_freshness_age_seconds";
pub const STALE_METRICS_TOTAL: &str = "tokeira_autoscaler_stale_metrics_total";
pub const DESIRED_REPLICAS: &str = "tokeira_autoscaler_service_desired_replicas";
pub const NOMINATION_TOTAL: &str = "tokeira_autoscaler_scale_in_nomination_total";
pub const ACTIVE_RECONCILER_LEASE_HELD: &str = "tokeira_autoscaler_active_reconciler_lease_held";
pub const MIMIR_QUERY_DURATION_SECONDS: &str = "tokeira_autoscaler_mimir_query_duration_seconds";

pub const METRIC_NAMES: &[(&str, MetricType)] = &[
    (LOOP_DURATION_SECONDS, MetricType::DurationHistogram),
    (SCALING_DECISIONS_TOTAL, MetricType::Counter),
    (METRIC_FRESHNESS_AGE_SECONDS, MetricType::DurationHistogram),
    (STALE_METRICS_TOTAL, MetricType::Counter),
    (DESIRED_REPLICAS, MetricType::Gauge),
    (NOMINATION_TOTAL, MetricType::Counter),
    (ACTIVE_RECONCILER_LEASE_HELD, MetricType::Gauge),
    (MIMIR_QUERY_DURATION_SECONDS, MetricType::DurationHistogram),
];

pub fn record_loop_duration(loop_name: AutoscalerLoopLabel, duration: std::time::Duration) {
    histogram!(LOOP_DURATION_SECONDS, "loop" => loop_name.as_str()).record(duration.as_secs_f64());
}

pub fn record_scaling_decision(
    loop_name: AutoscalerLoopLabel,
    direction: ScalingDirectionLabel,
    reason: &'static str,
) {
    counter!(
        SCALING_DECISIONS_TOTAL,
        "loop" => loop_name.as_str(),
        "direction" => direction.as_str(),
        "reason" => reason,
    )
    .increment(1);
}

pub fn record_metric_freshness_age(source: &'static str, age: std::time::Duration) {
    histogram!(METRIC_FRESHNESS_AGE_SECONDS, "source" => source).record(age.as_secs_f64());
}

pub fn record_stale_metrics(source: &'static str) {
    counter!(STALE_METRICS_TOTAL, "source" => source).increment(1);
}

pub fn set_desired_replicas(service: &str, replicas: u32) {
    gauge!(DESIRED_REPLICAS, "service" => service.to_owned()).set(f64::from(replicas));
}

pub fn record_nomination(outcome: NominationOutcomeLabel) {
    counter!(NOMINATION_TOTAL, "outcome" => outcome.as_str()).increment(1);
}

pub fn set_active_reconciler_lease_held(held: bool) {
    gauge!(ACTIVE_RECONCILER_LEASE_HELD).set(if held { 1.0 } else { 0.0 });
}

pub fn record_mimir_query_duration(
    query_kind: &'static str,
    outcome: OutcomeLabel,
    duration: std::time::Duration,
) {
    histogram!(
        MIMIR_QUERY_DURATION_SECONDS,
        "query_kind" => query_kind,
        "outcome" => outcome.as_str(),
    )
    .record(duration.as_secs_f64());
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
            record_loop_duration(
                AutoscalerLoopLabel::Replica,
                std::time::Duration::from_millis(9),
            );
            record_scaling_decision(
                AutoscalerLoopLabel::ScaleOut,
                ScalingDirectionLabel::Up,
                "broad_saturation",
            );
            record_metric_freshness_age("mimir", std::time::Duration::from_secs(4));
            record_stale_metrics("service_metrics");
            set_desired_replicas("tokeira-runtime", 3);
            record_nomination(NominationOutcomeLabel::Accepted);
            set_active_reconciler_lease_held(true);
            record_mimir_query_duration(
                "instant_value",
                OutcomeLabel::Success,
                std::time::Duration::from_millis(12),
            );
        });

        let snapshot = snapshot_map(&recorder);
        assert!(snapshot.contains_key(LOOP_DURATION_SECONDS));

        let (labels, value) = snapshot.get(SCALING_DECISIONS_TOTAL).unwrap();
        assert_eq!(labels.get("loop"), Some(&"scale_out".to_string()));
        assert_eq!(labels.get("direction"), Some(&"up".to_string()));
        assert_eq!(labels.get("reason"), Some(&"broad_saturation".to_string()));
        assert_eq!(value, &DebugValue::Counter(1));

        let (labels, _) = snapshot.get(METRIC_FRESHNESS_AGE_SECONDS).unwrap();
        assert_eq!(labels.get("source"), Some(&"mimir".to_string()));

        let (labels, value) = snapshot.get(STALE_METRICS_TOTAL).unwrap();
        assert_eq!(labels.get("source"), Some(&"service_metrics".to_string()));
        assert_eq!(value, &DebugValue::Counter(1));

        let (labels, value) = snapshot.get(DESIRED_REPLICAS).unwrap();
        assert_eq!(labels.get("service"), Some(&"tokeira-runtime".to_string()));
        assert_eq!(value, &DebugValue::Gauge(3.0.into()));

        let (labels, value) = snapshot.get(NOMINATION_TOTAL).unwrap();
        assert_eq!(labels.get("outcome"), Some(&"accepted".to_string()));
        assert_eq!(value, &DebugValue::Counter(1));

        assert_eq!(
            snapshot.get(ACTIVE_RECONCILER_LEASE_HELD).unwrap().1,
            DebugValue::Gauge(1.0.into())
        );

        let (labels, _) = snapshot.get(MIMIR_QUERY_DURATION_SECONDS).unwrap();
        assert_eq!(labels.get("query_kind"), Some(&"instant_value".to_string()));
        assert_eq!(labels.get("outcome"), Some(&"success".to_string()));
    }
}
