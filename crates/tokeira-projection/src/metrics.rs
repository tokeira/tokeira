//! Projection metric definitions and recording helpers.

use metrics::{counter, gauge, histogram};
use tokeira_observability::{ProjectionErrorKindLabel, ProjectionOutcomeLabel};
use tokeira_types::MetricType;
#[cfg(test)]
use tokeira_types::validate_metric_name;

pub const RECORDS_PROCESSED_TOTAL: &str = "tokeira_projection_records_processed_total";
pub const LAG_RECORDS: &str = "tokeira_projection_worker_lag_records";
pub const SINK_WRITE_DURATION_SECONDS: &str = "tokeira_projection_sink_write_duration_seconds";
pub const SINK_ERROR_TOTAL: &str = "tokeira_projection_sink_error_total";
pub const VISIBILITY_QUERY_DURATION_SECONDS: &str =
    "tokeira_projection_visibility_query_duration_seconds";
pub const SA_INDEX_SCAN_DURATION_SECONDS: &str =
    "tokeira_projection_sa_index_scan_duration_seconds";
pub const CHECKPOINT_WRITE_DURATION_SECONDS: &str =
    "tokeira_projection_checkpoint_write_duration_seconds";
pub const CHECKPOINT_LAG_SECONDS: &str = "tokeira_projection_checkpoint_lag_seconds";
pub const CHECKPOINT_TRANSITION_SEQUENCE: &str =
    "tokeira_projection_checkpoint_transition_sequence";
pub const LATEST_TRANSITION_SEQUENCE: &str = "tokeira_projection_latest_transition_sequence";
pub const WORKER_BATCH_RECORDS: &str = "tokeira_projection_worker_batch_records";
pub const POLL_EMPTY_TOTAL: &str = "tokeira_projection_poll_empty_total";

pub const METRIC_NAMES: &[(&str, MetricType)] = &[
    (RECORDS_PROCESSED_TOTAL, MetricType::Counter),
    (LAG_RECORDS, MetricType::Gauge),
    (SINK_WRITE_DURATION_SECONDS, MetricType::DurationHistogram),
    (SINK_ERROR_TOTAL, MetricType::Counter),
    (
        VISIBILITY_QUERY_DURATION_SECONDS,
        MetricType::DurationHistogram,
    ),
    (
        SA_INDEX_SCAN_DURATION_SECONDS,
        MetricType::DurationHistogram,
    ),
    (
        CHECKPOINT_WRITE_DURATION_SECONDS,
        MetricType::DurationHistogram,
    ),
    (CHECKPOINT_LAG_SECONDS, MetricType::Gauge),
    (CHECKPOINT_TRANSITION_SEQUENCE, MetricType::Gauge),
    (LATEST_TRANSITION_SEQUENCE, MetricType::Gauge),
    (WORKER_BATCH_RECORDS, MetricType::Histogram),
    (POLL_EMPTY_TOTAL, MetricType::Counter),
];

pub fn record_records_processed(partition_id: u32, count: usize) {
    record_records_processed_with_outcome(partition_id, count, ProjectionOutcomeLabel::Success);
}

pub fn record_records_processed_with_outcome(
    partition_id: u32,
    count: usize,
    outcome: ProjectionOutcomeLabel,
) {
    counter!(
        RECORDS_PROCESSED_TOTAL,
        "partition_id" => partition_id.to_string(),
        "outcome" => outcome.as_str(),
    )
    .increment(count as u64);
}

pub fn set_projection_lag(partition_id: u32, lag_records: usize) {
    gauge!(LAG_RECORDS, "partition_id" => partition_id.to_string()).set(lag_records as f64);
}

pub fn record_sink_write_duration(partition_id: u32, duration: std::time::Duration) {
    histogram!(
        SINK_WRITE_DURATION_SECONDS,
        "partition_id" => partition_id.to_string(),
    )
    .record(duration.as_secs_f64());
}

pub fn record_sink_error(partition_id: u32) {
    record_sink_error_with_kind(partition_id, ProjectionErrorKindLabel::Unknown);
}

pub fn record_sink_error_with_kind(partition_id: u32, error_kind: ProjectionErrorKindLabel) {
    counter!(
        SINK_ERROR_TOTAL,
        "partition_id" => partition_id.to_string(),
        "error_kind" => error_kind.as_str(),
    )
    .increment(1);
}

pub fn record_visibility_query_duration(query_type: &'static str, duration: std::time::Duration) {
    histogram!(VISIBILITY_QUERY_DURATION_SECONDS, "query_type" => query_type)
        .record(duration.as_secs_f64());
}

pub fn record_sa_index_scan_duration(index_table: &'static str, duration: std::time::Duration) {
    histogram!(SA_INDEX_SCAN_DURATION_SECONDS, "index_table" => index_table)
        .record(duration.as_secs_f64());
}

pub fn record_checkpoint_write_duration(partition_id: u32, duration: std::time::Duration) {
    histogram!(
        CHECKPOINT_WRITE_DURATION_SECONDS,
        "partition_id" => partition_id.to_string(),
    )
    .record(duration.as_secs_f64());
}

pub fn record_checkpoint_lag(duration: std::time::Duration) {
    gauge!(CHECKPOINT_LAG_SECONDS).set(duration.as_secs_f64());
}

pub fn set_checkpoint_transition_sequence(partition_id: u32, sequence: u64) {
    gauge!(
        CHECKPOINT_TRANSITION_SEQUENCE,
        "partition_id" => partition_id.to_string(),
    )
    .set(sequence as f64);
}

pub fn set_latest_transition_sequence(partition_id: u32, sequence: u64) {
    gauge!(
        LATEST_TRANSITION_SEQUENCE,
        "partition_id" => partition_id.to_string(),
    )
    .set(sequence as f64);
}

pub fn record_worker_batch_records(partition_id: u32, count: usize) {
    histogram!(
        WORKER_BATCH_RECORDS,
        "partition_id" => partition_id.to_string(),
    )
    .record(count as f64);
}

pub fn record_poll_empty(partition_id: u32) {
    counter!(POLL_EMPTY_TOTAL, "partition_id" => partition_id.to_string()).increment(1);
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
            record_records_processed(2, 5);
            set_projection_lag(2, 17);
            record_sink_write_duration(2, std::time::Duration::from_millis(14));
            record_sink_error(2);
            record_visibility_query_duration("list", std::time::Duration::from_millis(6));
            record_sa_index_scan_duration("sa_keyword_idx", std::time::Duration::from_millis(4));
            record_checkpoint_write_duration(2, std::time::Duration::from_millis(3));
            record_checkpoint_lag(std::time::Duration::from_millis(8));
            set_checkpoint_transition_sequence(2, 42);
            set_latest_transition_sequence(2, 45);
            record_worker_batch_records(2, 7);
            record_poll_empty(2);
        });

        let snapshot = snapshot_map(&recorder);

        let (labels, value) = snapshot.get(RECORDS_PROCESSED_TOTAL).unwrap();
        assert_eq!(labels.get("partition_id"), Some(&"2".to_string()));
        assert_eq!(labels.get("outcome"), Some(&"success".to_string()));
        assert_eq!(value, &DebugValue::Counter(5));

        let (labels, value) = snapshot.get(LAG_RECORDS).unwrap();
        assert_eq!(labels.get("partition_id"), Some(&"2".to_string()));
        assert_eq!(value, &DebugValue::Gauge(17.0.into()));

        let (labels, value) = snapshot.get(SINK_WRITE_DURATION_SECONDS).unwrap();
        assert_eq!(labels.get("partition_id"), Some(&"2".to_string()));
        match value {
            DebugValue::Histogram(values) => {
                assert_eq!(values.len(), 1);
                assert_eq!(values[0].into_inner(), 0.014f64);
            }
            other => panic!("expected histogram, got {other:?}"),
        }

        let (labels, value) = snapshot.get(SINK_ERROR_TOTAL).unwrap();
        assert_eq!(labels.get("partition_id"), Some(&"2".to_string()));
        assert_eq!(labels.get("error_kind"), Some(&"unknown".to_string()));
        assert_eq!(value, &DebugValue::Counter(1));

        let (labels, value) = snapshot.get(VISIBILITY_QUERY_DURATION_SECONDS).unwrap();
        assert_eq!(labels.get("query_type"), Some(&"list".to_string()));
        match value {
            DebugValue::Histogram(values) => assert_eq!(values[0].into_inner(), 0.006f64),
            other => panic!("expected histogram, got {other:?}"),
        }

        let (labels, value) = snapshot.get(SA_INDEX_SCAN_DURATION_SECONDS).unwrap();
        assert_eq!(
            labels.get("index_table"),
            Some(&"sa_keyword_idx".to_string())
        );
        match value {
            DebugValue::Histogram(values) => assert_eq!(values[0].into_inner(), 0.004f64),
            other => panic!("expected histogram, got {other:?}"),
        }

        let (labels, value) = snapshot.get(CHECKPOINT_WRITE_DURATION_SECONDS).unwrap();
        assert_eq!(labels.get("partition_id"), Some(&"2".to_string()));
        match value {
            DebugValue::Histogram(values) => assert_eq!(values[0].into_inner(), 0.003f64),
            other => panic!("expected histogram, got {other:?}"),
        }

        let (_, value) = snapshot.get(CHECKPOINT_LAG_SECONDS).unwrap();
        assert_eq!(value, &DebugValue::Gauge(0.008f64.into()));

        let (labels, value) = snapshot.get(CHECKPOINT_TRANSITION_SEQUENCE).unwrap();
        assert_eq!(labels.get("partition_id"), Some(&"2".to_string()));
        assert_eq!(value, &DebugValue::Gauge(42.0.into()));

        let (labels, value) = snapshot.get(LATEST_TRANSITION_SEQUENCE).unwrap();
        assert_eq!(labels.get("partition_id"), Some(&"2".to_string()));
        assert_eq!(value, &DebugValue::Gauge(45.0.into()));

        let (labels, value) = snapshot.get(WORKER_BATCH_RECORDS).unwrap();
        assert_eq!(labels.get("partition_id"), Some(&"2".to_string()));
        match value {
            DebugValue::Histogram(values) => assert_eq!(values[0].into_inner(), 7.0f64),
            other => panic!("expected histogram, got {other:?}"),
        }

        let (labels, value) = snapshot.get(POLL_EMPTY_TOTAL).unwrap();
        assert_eq!(labels.get("partition_id"), Some(&"2".to_string()));
        assert_eq!(value, &DebugValue::Counter(1));
    }
}
