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
pub const DSQL_CLASS_PERMIT_WAIT_DURATION_SECONDS: &str =
    "tokeira_dsql_class_permit_wait_duration_seconds";
pub const DSQL_POOL_WAITING: &str = "tokeira_dsql_pool_waiting";
pub const DSQL_OPERATION_DURATION_SECONDS: &str = "tokeira_storage_dsql_operation_duration_seconds";
pub const DSQL_STATEMENT_DURATION_SECONDS: &str = "tokeira_storage_dsql_statement_duration_seconds";
pub const DSQL_OCC_CONFLICT_TOTAL: &str = "tokeira_storage_dsql_occ_conflict_total";
pub const DSQL_RETRY_TOTAL: &str = "tokeira_storage_dsql_retry_total";
pub const DSQL_OPERATION_TOTAL: &str = "tokeira_storage_dsql_operation_total";
pub const DSQL_RESERVOIR_IN_FLIGHT: &str = "tokeira_dsql_reservoir_in_flight";
pub const DSQL_PROJECTION_READ_DURATION_SECONDS: &str =
    "tokeira_storage_dsql_projection_read_duration_seconds";
pub const DSQL_PROJECTION_BATCH_SIZE: &str = "tokeira_storage_dsql_projection_batch_size";
pub const DSQL_RESERVOIR_CONNECTION_CREATE_DURATION_SECONDS: &str =
    "tokeira_dsql_reservoir_connection_create_duration_seconds";
pub const DSQL_RESERVOIR_CONNECTION_VALIDATE_DURATION_SECONDS: &str =
    "tokeira_dsql_reservoir_connection_validate_duration_seconds";
pub const DSQL_RESERVOIR_CONNECTION_AGE_SECONDS: &str =
    "tokeira_dsql_reservoir_connection_age_seconds";
pub const DSQL_RATE_LIMITER_TOKENS_REMAINING: &str = "tokeira_dsql_rate_limiter_tokens_remaining";
pub const DSQL_RATE_LIMITER_THROTTLED_TOTAL: &str = "tokeira_dsql_rate_limiter_throttled_total";
pub const DSQL_RATE_LIMITER_THROTTLE_DURATION_SECONDS: &str =
    "tokeira_dsql_rate_limiter_throttle_duration_seconds";
pub const DSQL_QUERY_DURATION_SECONDS: &str = "tokeira_storage_dsql_query_duration_seconds";
pub const DSQL_ROWS_READ: &str = "tokeira_storage_dsql_rows_read";
pub const DSQL_ROWS_WRITTEN: &str = "tokeira_storage_dsql_rows_written";
pub const DSQL_COMMIT_RETRIES: &str = "tokeira_storage_dsql_commit_retries";
pub const DSQL_RESERVOIR_UTILIZATION_RATIO: &str = "tokeira_dsql_reservoir_utilization_ratio";
pub const DSQL_SHARD_OPERATION_TOTAL: &str = "tokeira_storage_dsql_shard_operation_total";
pub const DSQL_SHARD_CONFLICT_TOTAL: &str = "tokeira_storage_dsql_shard_conflict_total";
pub const DSQL_SHARD_DURATION_SECONDS: &str = "tokeira_storage_dsql_shard_duration_seconds";
pub const DSQL_CONNECTION_ERROR_TOTAL: &str = "tokeira_dsql_connection_error_total";
pub const DSQL_ERROR_CODE_TOTAL: &str = "tokeira_dsql_error_code_total";

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
    (
        DSQL_CLASS_PERMIT_WAIT_DURATION_SECONDS,
        MetricType::DurationHistogram,
    ),
    (DSQL_POOL_WAITING, MetricType::Gauge),
    (
        DSQL_OPERATION_DURATION_SECONDS,
        MetricType::DurationHistogram,
    ),
    (
        DSQL_STATEMENT_DURATION_SECONDS,
        MetricType::DurationHistogram,
    ),
    (DSQL_OCC_CONFLICT_TOTAL, MetricType::Counter),
    (DSQL_RETRY_TOTAL, MetricType::Counter),
    (DSQL_OPERATION_TOTAL, MetricType::Counter),
    (DSQL_RESERVOIR_IN_FLIGHT, MetricType::Gauge),
    (
        DSQL_PROJECTION_READ_DURATION_SECONDS,
        MetricType::DurationHistogram,
    ),
    (DSQL_PROJECTION_BATCH_SIZE, MetricType::Histogram),
    (
        DSQL_RESERVOIR_CONNECTION_CREATE_DURATION_SECONDS,
        MetricType::DurationHistogram,
    ),
    (
        DSQL_RESERVOIR_CONNECTION_VALIDATE_DURATION_SECONDS,
        MetricType::DurationHistogram,
    ),
    (
        DSQL_RESERVOIR_CONNECTION_AGE_SECONDS,
        MetricType::DurationHistogram,
    ),
    (DSQL_RATE_LIMITER_TOKENS_REMAINING, MetricType::Gauge),
    (DSQL_RATE_LIMITER_THROTTLED_TOTAL, MetricType::Counter),
    (
        DSQL_RATE_LIMITER_THROTTLE_DURATION_SECONDS,
        MetricType::DurationHistogram,
    ),
    (DSQL_QUERY_DURATION_SECONDS, MetricType::DurationHistogram),
    (DSQL_ROWS_READ, MetricType::Histogram),
    (DSQL_ROWS_WRITTEN, MetricType::Histogram),
    (DSQL_COMMIT_RETRIES, MetricType::Histogram),
    (DSQL_RESERVOIR_UTILIZATION_RATIO, MetricType::Gauge),
    (DSQL_SHARD_OPERATION_TOTAL, MetricType::Counter),
    (DSQL_SHARD_CONFLICT_TOTAL, MetricType::Counter),
    (DSQL_SHARD_DURATION_SECONDS, MetricType::DurationHistogram),
    (DSQL_CONNECTION_ERROR_TOTAL, MetricType::Counter),
    (DSQL_ERROR_CODE_TOTAL, MetricType::Counter),
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

pub fn increment_dsql_pool_waiting(class: &'static str) {
    gauge!(DSQL_POOL_WAITING, "class" => class).increment(1.0);
}

pub fn decrement_dsql_pool_waiting(class: &'static str) {
    gauge!(DSQL_POOL_WAITING, "class" => class).decrement(1.0);
}

pub fn record_dsql_class_permit_wait_duration(class: &'static str, duration: std::time::Duration) {
    histogram!(DSQL_CLASS_PERMIT_WAIT_DURATION_SECONDS, "class" => class)
        .record(duration.as_secs_f64());
}

pub fn record_dsql_operation_duration(
    operation: &'static str,
    outcome: &'static str,
    duration: std::time::Duration,
) {
    histogram!(DSQL_OPERATION_DURATION_SECONDS, "operation" => operation, "outcome" => outcome)
        .record(duration.as_secs_f64());
}

pub fn record_dsql_statement_duration(
    operation: &'static str,
    statement: &'static str,
    duration: std::time::Duration,
) {
    histogram!(DSQL_STATEMENT_DURATION_SECONDS, "operation" => operation, "statement" => statement)
        .record(duration.as_secs_f64());
}

pub fn record_dsql_occ_conflict(operation: &'static str) {
    counter!(DSQL_OCC_CONFLICT_TOTAL, "operation" => operation).increment(1);
}

pub fn record_dsql_retry(operation: &'static str, outcome: &'static str) {
    counter!(DSQL_RETRY_TOTAL, "operation" => operation, "outcome" => outcome).increment(1);
}

pub fn record_dsql_operation_total(operation: &'static str, outcome: &'static str) {
    counter!(DSQL_OPERATION_TOTAL, "operation" => operation, "outcome" => outcome).increment(1);
}

pub fn set_dsql_reservoir_in_flight(count: usize) {
    gauge!(DSQL_RESERVOIR_IN_FLIGHT).set(count as f64);
}

pub fn record_dsql_projection_read_duration(partition_id: u32, duration: std::time::Duration) {
    histogram!(DSQL_PROJECTION_READ_DURATION_SECONDS, "partition_id" => partition_id.to_string())
        .record(duration.as_secs_f64());
}

pub fn record_dsql_projection_batch_size(partition_id: u32, batch_size: usize) {
    histogram!(DSQL_PROJECTION_BATCH_SIZE, "partition_id" => partition_id.to_string())
        .record(batch_size as f64);
}

pub fn record_dsql_reservoir_connection_create_duration(duration: std::time::Duration) {
    histogram!(DSQL_RESERVOIR_CONNECTION_CREATE_DURATION_SECONDS).record(duration.as_secs_f64());
}

pub fn record_dsql_reservoir_connection_validate_duration(duration: std::time::Duration) {
    histogram!(DSQL_RESERVOIR_CONNECTION_VALIDATE_DURATION_SECONDS).record(duration.as_secs_f64());
}

pub fn record_dsql_reservoir_connection_age(reason: &'static str, age: std::time::Duration) {
    histogram!(DSQL_RESERVOIR_CONNECTION_AGE_SECONDS, "retirement_reason" => reason)
        .record(age.as_secs_f64());
}

pub fn set_dsql_rate_limiter_tokens_remaining(tokens: f64) {
    gauge!(DSQL_RATE_LIMITER_TOKENS_REMAINING).set(tokens);
}

pub fn record_dsql_rate_limiter_throttled() {
    counter!(DSQL_RATE_LIMITER_THROTTLED_TOTAL).increment(1);
}

pub fn record_dsql_rate_limiter_throttle_duration(duration: std::time::Duration) {
    histogram!(DSQL_RATE_LIMITER_THROTTLE_DURATION_SECONDS).record(duration.as_secs_f64());
}

pub fn record_dsql_query_duration(
    operation: &'static str,
    outcome: &'static str,
    duration: std::time::Duration,
) {
    histogram!(DSQL_QUERY_DURATION_SECONDS, "operation" => operation, "outcome" => outcome)
        .record(duration.as_secs_f64());
}

pub fn record_dsql_rows_read(operation: &'static str, count: usize) {
    histogram!(DSQL_ROWS_READ, "operation" => operation).record(count as f64);
}

pub fn record_dsql_rows_written(operation: &'static str, count: u64) {
    histogram!(DSQL_ROWS_WRITTEN, "operation" => operation).record(count as f64);
}

pub fn record_dsql_commit_retries(retries: u32) {
    histogram!(DSQL_COMMIT_RETRIES).record(f64::from(retries));
}

pub fn set_dsql_reservoir_utilization_ratio(in_flight: usize, ready: usize) {
    let total = in_flight + ready;
    let ratio = if total == 0 {
        0.0
    } else {
        in_flight as f64 / total as f64
    };
    gauge!(DSQL_RESERVOIR_UTILIZATION_RATIO).set(ratio);
}

pub fn record_dsql_shard_operation(shard_id: u32, operation: &'static str) {
    counter!(DSQL_SHARD_OPERATION_TOTAL, "shard_id" => shard_id.to_string(), "operation" => operation)
        .increment(1);
}

pub fn record_dsql_shard_conflict(shard_id: u32) {
    counter!(DSQL_SHARD_CONFLICT_TOTAL, "shard_id" => shard_id.to_string()).increment(1);
}

pub fn record_dsql_shard_duration(shard_id: u32, duration: std::time::Duration) {
    histogram!(DSQL_SHARD_DURATION_SECONDS, "shard_id" => shard_id.to_string())
        .record(duration.as_secs_f64());
}

pub fn record_dsql_connection_error(error_kind: &'static str) {
    counter!(DSQL_CONNECTION_ERROR_TOTAL, "error_kind" => error_kind).increment(1);
}

pub fn record_dsql_error_code(sqlstate: &str) {
    counter!(DSQL_ERROR_CODE_TOTAL, "sqlstate" => sqlstate.to_owned()).increment(1);
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use metrics::{counter, with_local_recorder};
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use proptest::prelude::*;

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
            increment_dsql_pool_waiting("commit");
            decrement_dsql_pool_waiting("commit");
            record_dsql_class_permit_wait_duration("commit", std::time::Duration::from_millis(3));
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
        assert!(snapshot.contains_key(DSQL_POOL_WAITING));
        assert!(snapshot.contains_key(DSQL_CLASS_PERMIT_WAIT_DURATION_SECONDS));
    }

    #[test]
    fn dsql_deep_helpers_emit_expected_metrics_and_labels() {
        let recorder = DebuggingRecorder::new();

        with_local_recorder(&recorder, || {
            record_dsql_operation_duration(
                "load_run",
                "success",
                std::time::Duration::from_millis(12),
            );
            record_dsql_statement_duration(
                "commit_transition",
                "load_hot",
                std::time::Duration::from_millis(5),
            );
            record_dsql_occ_conflict("commit_transition");
            record_dsql_retry("commit_transition_for_bundle", "success");
            record_dsql_operation_total("load_run", "success");
            set_dsql_reservoir_in_flight(3);
            record_dsql_projection_read_duration(7, std::time::Duration::from_millis(21));
            record_dsql_projection_batch_size(7, 42);
            record_dsql_reservoir_connection_create_duration(std::time::Duration::from_millis(31));
            record_dsql_reservoir_connection_validate_duration(std::time::Duration::from_millis(4));
            record_dsql_reservoir_connection_age("expired", std::time::Duration::from_secs(120));
            set_dsql_rate_limiter_tokens_remaining(5.5);
            record_dsql_rate_limiter_throttled();
            record_dsql_rate_limiter_throttle_duration(std::time::Duration::from_millis(8));
            record_dsql_query_duration("load_run", "success", std::time::Duration::from_millis(10));
            record_dsql_rows_read("read_history", 9);
            record_dsql_rows_written("persist_to_backlog", 2);
            record_dsql_commit_retries(1);
            set_dsql_reservoir_utilization_ratio(2, 6);
            record_dsql_shard_operation(4, "load_run");
            record_dsql_shard_conflict(4);
            record_dsql_shard_duration(4, std::time::Duration::from_millis(16));
            record_dsql_connection_error("timeout");
            record_dsql_error_code("40001");
        });

        let snapshot = snapshot_map(&recorder);
        assert!(snapshot.contains_key(DSQL_OPERATION_DURATION_SECONDS));
        assert!(snapshot.contains_key(DSQL_STATEMENT_DURATION_SECONDS));
        assert!(snapshot.contains_key(DSQL_OCC_CONFLICT_TOTAL));
        assert!(snapshot.contains_key(DSQL_RETRY_TOTAL));
        assert!(snapshot.contains_key(DSQL_OPERATION_TOTAL));
        assert!(snapshot.contains_key(DSQL_RESERVOIR_IN_FLIGHT));
        assert!(snapshot.contains_key(DSQL_PROJECTION_READ_DURATION_SECONDS));
        assert!(snapshot.contains_key(DSQL_PROJECTION_BATCH_SIZE));
        assert!(snapshot.contains_key(DSQL_RESERVOIR_CONNECTION_CREATE_DURATION_SECONDS));
        assert!(snapshot.contains_key(DSQL_RESERVOIR_CONNECTION_VALIDATE_DURATION_SECONDS));
        assert!(snapshot.contains_key(DSQL_RESERVOIR_CONNECTION_AGE_SECONDS));
        assert!(snapshot.contains_key(DSQL_RATE_LIMITER_TOKENS_REMAINING));
        assert!(snapshot.contains_key(DSQL_RATE_LIMITER_THROTTLED_TOTAL));
        assert!(snapshot.contains_key(DSQL_RATE_LIMITER_THROTTLE_DURATION_SECONDS));
        assert!(snapshot.contains_key(DSQL_QUERY_DURATION_SECONDS));
        assert!(snapshot.contains_key(DSQL_ROWS_READ));
        assert!(snapshot.contains_key(DSQL_ROWS_WRITTEN));
        assert!(snapshot.contains_key(DSQL_COMMIT_RETRIES));
        assert!(snapshot.contains_key(DSQL_RESERVOIR_UTILIZATION_RATIO));
        assert!(snapshot.contains_key(DSQL_SHARD_OPERATION_TOTAL));
        assert!(snapshot.contains_key(DSQL_SHARD_CONFLICT_TOTAL));
        assert!(snapshot.contains_key(DSQL_SHARD_DURATION_SECONDS));
        assert!(snapshot.contains_key(DSQL_CONNECTION_ERROR_TOTAL));
        assert!(snapshot.contains_key(DSQL_ERROR_CODE_TOTAL));

        let (labels, value) = snapshot.get(DSQL_RETRY_TOTAL).unwrap();
        assert_eq!(
            labels.get("operation"),
            Some(&"commit_transition_for_bundle".to_string())
        );
        assert_eq!(labels.get("outcome"), Some(&"success".to_string()));
        assert_eq!(value, &DebugValue::Counter(1));

        let (_, value) = snapshot.get(DSQL_RESERVOIR_UTILIZATION_RATIO).unwrap();
        assert_eq!(value, &DebugValue::Gauge(0.25.into()));
    }

    proptest! {
        #[test]
        fn counter_helpers_accumulate(count in 0u64..10_000) {
            let recorder = DebuggingRecorder::new();
            with_local_recorder(&recorder, || {
                counter!(DSQL_RETRY_TOTAL, "operation" => "commit_transition", "outcome" => "success").increment(count);
            });
            let snapshot = snapshot_map(&recorder);
            let (_, value) = snapshot.get(DSQL_RETRY_TOTAL).unwrap();
            prop_assert_eq!(value, &DebugValue::Counter(count));
        }

        #[test]
        fn histogram_helpers_record_each_observation(values in prop::collection::vec(0u64..10_000, 1..32)) {
            let recorder = DebuggingRecorder::new();
            with_local_recorder(&recorder, || {
                for value in &values {
                    record_dsql_rows_read("read_history", *value as usize);
                }
            });
            let snapshot = snapshot_map(&recorder);
            let (_, value) = snapshot.get(DSQL_ROWS_READ).unwrap();
            match value {
                DebugValue::Histogram(observations) => {
                    prop_assert_eq!(observations.len(), values.len());
                }
                _ => prop_assert!(false, "expected histogram"),
            }
        }

        #[test]
        fn gauge_helpers_are_last_write_wins(values in prop::collection::vec(0usize..10_000, 1..32)) {
            let recorder = DebuggingRecorder::new();
            with_local_recorder(&recorder, || {
                for value in &values {
                    set_dsql_reservoir_in_flight(*value);
                }
            });
            let snapshot = snapshot_map(&recorder);
            let (_, value) = snapshot.get(DSQL_RESERVOIR_IN_FLIGHT).unwrap();
            prop_assert_eq!(value, &DebugValue::Gauge((*values.last().unwrap() as f64).into()));
        }

        #[test]
        fn utilization_ratio_matches_inputs(in_flight in 0usize..10_000, ready in 0usize..10_000) {
            let recorder = DebuggingRecorder::new();
            with_local_recorder(&recorder, || {
                set_dsql_reservoir_utilization_ratio(in_flight, ready);
            });
            let snapshot = snapshot_map(&recorder);
            let (_, value) = snapshot.get(DSQL_RESERVOIR_UTILIZATION_RATIO).unwrap();
            let total = in_flight + ready;
            let expected = if total == 0 {
                0.0
            } else {
                in_flight as f64 / total as f64
            };
            prop_assert_eq!(value, &DebugValue::Gauge(expected.into()));
        }
    }
}
