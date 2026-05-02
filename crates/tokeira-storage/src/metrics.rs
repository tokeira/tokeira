//! Storage metric definitions and recording helpers.
//!
//! Metric names live in one manifest so naming validation can cover the whole
//! crate. Callers should use the small recording helpers instead of constructing
//! ad hoc metrics, which keeps label sets stable for dashboards and alerts.

use metrics::{counter, gauge, histogram};
use tokeira_types::MetricType;
#[cfg(test)]
use tokeira_types::validate_metric_name;

pub const COMMIT_TRANSITION_DURATION_SECONDS: &str =
    "tokeira_storage_commit_transition_duration_seconds";
pub const LOAD_RUN_DURATION_SECONDS: &str = "tokeira_storage_load_run_duration_seconds";
pub const READ_HISTORY_DURATION_SECONDS: &str = "tokeira_storage_read_history_duration_seconds";
pub const OPERATION_TOTAL: &str = "tokeira_storage_repository_operation_total";
pub const DSQL_POOL_CONNECTIONS_TOTAL: &str = "tokeira_dsql_pool_connections_total";
pub const DSQL_POOL_CHECKOUT_DURATION_SECONDS: &str = "tokeira_dsql_pool_checkout_duration_seconds";
pub const DSQL_POOL_EMPTY_RESERVOIR_TOTAL: &str = "tokeira_dsql_pool_empty_reservoir_total";
pub const DSQL_POOL_CONNECTIONS_CREATED_TOTAL: &str = "tokeira_dsql_pool_connections_created_total";
pub const DSQL_POOL_CONNECTIONS_RETIRED_TOTAL: &str = "tokeira_dsql_pool_connections_retired_total";
pub const DSQL_POOL_CONNECTIONS_RETURNED_TOTAL: &str =
    "tokeira_dsql_pool_connections_returned_total";
pub const DSQL_POOL_RATE_LIMITER_TOKENS: &str = "tokeira_dsql_pool_rate_limiter_tokens";
pub const DSQL_POOL_RATE_LIMITER_RATE: &str = "tokeira_dsql_pool_rate_limiter_rate";
pub const DSQL_POOL_CLASS_BUDGET_TOTAL: &str = "tokeira_dsql_pool_class_budget_total";
pub const DSQL_POOL_CLASS_IN_USE: &str = "tokeira_dsql_pool_class_in_use";
pub const DSQL_POOL_CLASS_WAITERS: &str = "tokeira_dsql_pool_class_waiters";

pub const METRIC_NAMES: &[(&str, MetricType)] = &[
    (
        COMMIT_TRANSITION_DURATION_SECONDS,
        MetricType::DurationHistogram,
    ),
    (LOAD_RUN_DURATION_SECONDS, MetricType::DurationHistogram),
    (READ_HISTORY_DURATION_SECONDS, MetricType::DurationHistogram),
    (OPERATION_TOTAL, MetricType::Counter),
    (DSQL_POOL_CONNECTIONS_TOTAL, MetricType::Gauge),
    (
        DSQL_POOL_CHECKOUT_DURATION_SECONDS,
        MetricType::DurationHistogram,
    ),
    (DSQL_POOL_EMPTY_RESERVOIR_TOTAL, MetricType::Counter),
    (DSQL_POOL_CONNECTIONS_CREATED_TOTAL, MetricType::Counter),
    (DSQL_POOL_CONNECTIONS_RETIRED_TOTAL, MetricType::Counter),
    (DSQL_POOL_CONNECTIONS_RETURNED_TOTAL, MetricType::Counter),
    (DSQL_POOL_RATE_LIMITER_TOKENS, MetricType::Gauge),
    (DSQL_POOL_RATE_LIMITER_RATE, MetricType::Gauge),
    (DSQL_POOL_CLASS_BUDGET_TOTAL, MetricType::Gauge),
    (DSQL_POOL_CLASS_IN_USE, MetricType::Gauge),
    (DSQL_POOL_CLASS_WAITERS, MetricType::Gauge),
];

pub fn record_commit_transition_duration(
    namespace: Option<String>,
    outcome: &'static str,
    duration: std::time::Duration,
) {
    // Unknown is explicit rather than omitted so dashboard aggregations do not
    // silently split on missing labels.
    let namespace = namespace.unwrap_or_else(|| "unknown".to_string());
    histogram!(
        COMMIT_TRANSITION_DURATION_SECONDS,
        "namespace" => namespace,
        "outcome" => outcome,
    )
    .record(duration.as_secs_f64());
}

pub fn record_load_run_duration(duration: std::time::Duration) {
    histogram!(LOAD_RUN_DURATION_SECONDS).record(duration.as_secs_f64());
}

pub fn record_read_history_duration(duration: std::time::Duration) {
    histogram!(READ_HISTORY_DURATION_SECONDS).record(duration.as_secs_f64());
}

pub fn record_storage_operation(operation: &'static str, outcome: &'static str) {
    counter!(OPERATION_TOTAL, "operation" => operation, "outcome" => outcome).increment(1);
}

pub fn record_dsql_pool_connections_total(count: usize) {
    gauge!(DSQL_POOL_CONNECTIONS_TOTAL).set(count as f64);
}

pub fn record_dsql_pool_checkout_duration(class: &'static str, duration: std::time::Duration) {
    histogram!(DSQL_POOL_CHECKOUT_DURATION_SECONDS, "class" => class)
        .record(duration.as_secs_f64());
}

pub fn record_dsql_pool_empty_reservoir() {
    counter!(DSQL_POOL_EMPTY_RESERVOIR_TOTAL).increment(1);
}

pub fn record_dsql_pool_connection_created() {
    counter!(DSQL_POOL_CONNECTIONS_CREATED_TOTAL).increment(1);
}

pub fn record_dsql_pool_connection_retired(reason: &'static str) {
    counter!(DSQL_POOL_CONNECTIONS_RETIRED_TOTAL, "reason" => reason).increment(1);
}

pub fn record_dsql_pool_connection_returned() {
    counter!(DSQL_POOL_CONNECTIONS_RETURNED_TOTAL).increment(1);
}

pub fn record_dsql_pool_rate_limiter(tokens: f64, rate: f64) {
    gauge!(DSQL_POOL_RATE_LIMITER_TOKENS).set(tokens);
    gauge!(DSQL_POOL_RATE_LIMITER_RATE).set(rate);
}

pub fn record_dsql_pool_class_budget(
    class: &'static str,
    total: usize,
    in_use: usize,
    waiters: usize,
) {
    // Class-budget metrics are recorded together so operators can compare
    // configured permits, live usage, and wait pressure on the same label set.
    gauge!(DSQL_POOL_CLASS_BUDGET_TOTAL, "class" => class).set(total as f64);
    gauge!(DSQL_POOL_CLASS_IN_USE, "class" => class).set(in_use as f64);
    gauge!(DSQL_POOL_CLASS_WAITERS, "class" => class).set(waiters as f64);
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
            record_commit_transition_duration(
                Some("default".to_string()),
                "success",
                std::time::Duration::from_millis(18),
            );
            record_load_run_duration(std::time::Duration::from_millis(7));
            record_read_history_duration(std::time::Duration::from_millis(9));
            record_storage_operation("load_run", "success");
            record_dsql_pool_connections_total(2);
            record_dsql_pool_checkout_duration("commit", std::time::Duration::from_millis(11));
            record_dsql_pool_empty_reservoir();
            record_dsql_pool_connection_created();
            record_dsql_pool_connection_retired("expired");
            record_dsql_pool_connection_returned();
            record_dsql_pool_rate_limiter(4.0, 100.0);
            record_dsql_pool_class_budget("commit", 5, 1, 0);
        });

        let snapshot = snapshot_map(&recorder);

        let (labels, value) = snapshot.get(COMMIT_TRANSITION_DURATION_SECONDS).unwrap();
        assert_eq!(labels.get("namespace"), Some(&"default".to_string()));
        assert_eq!(labels.get("outcome"), Some(&"success".to_string()));
        match value {
            DebugValue::Histogram(values) => {
                assert_eq!(values.len(), 1);
                assert_eq!(values[0].into_inner(), 0.018f64);
            }
            other => panic!("expected histogram, got {other:?}"),
        }

        match &snapshot.get(LOAD_RUN_DURATION_SECONDS).unwrap().1 {
            DebugValue::Histogram(values) => {
                assert_eq!(values.len(), 1);
                assert_eq!(values[0].into_inner(), 0.007f64);
            }
            other => panic!("expected histogram, got {other:?}"),
        }

        match &snapshot.get(READ_HISTORY_DURATION_SECONDS).unwrap().1 {
            DebugValue::Histogram(values) => {
                assert_eq!(values.len(), 1);
                assert_eq!(values[0].into_inner(), 0.009f64);
            }
            other => panic!("expected histogram, got {other:?}"),
        }

        let (labels, value) = snapshot.get(OPERATION_TOTAL).unwrap();
        assert_eq!(labels.get("operation"), Some(&"load_run".to_string()));
        assert_eq!(labels.get("outcome"), Some(&"success".to_string()));
        assert_eq!(value, &DebugValue::Counter(1));

        assert!(snapshot.contains_key(DSQL_POOL_CONNECTIONS_TOTAL));
        assert!(snapshot.contains_key(DSQL_POOL_CHECKOUT_DURATION_SECONDS));
        assert!(snapshot.contains_key(DSQL_POOL_EMPTY_RESERVOIR_TOTAL));
        assert!(snapshot.contains_key(DSQL_POOL_CONNECTIONS_CREATED_TOTAL));
        assert!(snapshot.contains_key(DSQL_POOL_CONNECTIONS_RETIRED_TOTAL));
        assert!(snapshot.contains_key(DSQL_POOL_CONNECTIONS_RETURNED_TOTAL));
        assert!(snapshot.contains_key(DSQL_POOL_RATE_LIMITER_TOKENS));
        assert!(snapshot.contains_key(DSQL_POOL_RATE_LIMITER_RATE));
        assert!(snapshot.contains_key(DSQL_POOL_CLASS_BUDGET_TOTAL));
        assert!(snapshot.contains_key(DSQL_POOL_CLASS_IN_USE));
        assert!(snapshot.contains_key(DSQL_POOL_CLASS_WAITERS));
    }
}
