//! Projection metric definitions and recording helpers.

use metrics::{counter, gauge, histogram};
use tokeira_observability::{
    ProjectionErrorKindLabel, ProjectionFailureKindLabel, ProjectionOutcomeLabel,
};
use tokeira_types::MetricType;
#[cfg(test)]
use tokeira_types::validate_metric_name;

pub const RECORDS_PROCESSED_TOTAL: &str = "tokeira_projection_records_processed_total";
pub const SINK_WRITE_DURATION_SECONDS: &str = "tokeira_projection_sink_write_duration_seconds";
pub const SINK_ERROR_TOTAL: &str = "tokeira_projection_sink_error_total";
pub const VISIBILITY_QUERY_DURATION_SECONDS: &str =
    "tokeira_projection_visibility_query_duration_seconds";
pub const SA_INDEX_SCAN_DURATION_SECONDS: &str =
    "tokeira_projection_sa_index_scan_duration_seconds";
pub const IMAGES_READ_TOTAL: &str = "tokeira_projection_images_read_total";
pub const IMAGES_DELETED_TOTAL: &str = "tokeira_projection_images_deleted_total";
pub const PASS_FAILURES_TOTAL: &str = "tokeira_projection_pass_failures_total";
pub const PASS_DURATION_SECONDS: &str = "tokeira_projection_pass_duration_seconds";
pub const OLDEST_IMAGE_AGE_SECONDS: &str = "tokeira_projection_oldest_image_age_seconds";

pub const METRIC_NAMES: &[(&str, MetricType)] = &[
    (RECORDS_PROCESSED_TOTAL, MetricType::Counter),
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
    (IMAGES_READ_TOTAL, MetricType::Counter),
    (IMAGES_DELETED_TOTAL, MetricType::Counter),
    (PASS_FAILURES_TOTAL, MetricType::Counter),
    (PASS_DURATION_SECONDS, MetricType::DurationHistogram),
    (OLDEST_IMAGE_AGE_SECONDS, MetricType::Gauge),
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

/// Count the images a pass read from its partition.
pub fn record_images_read(partition_id: u32, count: usize) {
    counter!(IMAGES_READ_TOTAL, "partition_id" => partition_id.to_string()).increment(count as u64);
}

/// Count the images a pass deleted from its partition.
pub fn record_images_deleted(partition_id: u32, count: u64) {
    counter!(IMAGES_DELETED_TOTAL, "partition_id" => partition_id.to_string()).increment(count);
}

/// Count the failures of a pass's reads, applies or deletes.
pub fn record_pass_failures(partition_id: u32, kind: ProjectionFailureKindLabel, count: usize) {
    counter!(
        PASS_FAILURES_TOTAL,
        "partition_id" => partition_id.to_string(),
        "kind" => kind.as_str(),
    )
    .increment(count as u64);
}

pub fn record_pass_duration(partition_id: u32, duration: std::time::Duration) {
    histogram!(
        PASS_DURATION_SECONDS,
        "partition_id" => partition_id.to_string(),
    )
    .record(duration.as_secs_f64());
}

/// Set how long the oldest image a pass read had waited in the log.
pub fn set_oldest_image_age(partition_id: u32, age: std::time::Duration) {
    gauge!(
        OLDEST_IMAGE_AGE_SECONDS,
        "partition_id" => partition_id.to_string(),
    )
    .set(age.as_secs_f64());
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
            record_sink_write_duration(2, std::time::Duration::from_millis(14));
            record_sink_error(2);
            record_visibility_query_duration("list", std::time::Duration::from_millis(6));
            record_sa_index_scan_duration("sa_keyword_idx", std::time::Duration::from_millis(4));
            record_images_read(2, 7);
            record_images_deleted(2, 6);
            record_pass_failures(2, ProjectionFailureKindLabel::Delete, 1);
            record_pass_duration(2, std::time::Duration::from_millis(3));
            set_oldest_image_age(2, std::time::Duration::from_millis(8));
        });

        let snapshot = snapshot_map(&recorder);

        let (labels, value) = snapshot.get(RECORDS_PROCESSED_TOTAL).unwrap();
        assert_eq!(labels.get("partition_id"), Some(&"2".to_string()));
        assert_eq!(labels.get("outcome"), Some(&"success".to_string()));
        assert_eq!(value, &DebugValue::Counter(5));

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

        let (labels, value) = snapshot.get(IMAGES_READ_TOTAL).unwrap();
        assert_eq!(labels.get("partition_id"), Some(&"2".to_string()));
        assert_eq!(value, &DebugValue::Counter(7));

        let (labels, value) = snapshot.get(IMAGES_DELETED_TOTAL).unwrap();
        assert_eq!(labels.get("partition_id"), Some(&"2".to_string()));
        assert_eq!(value, &DebugValue::Counter(6));

        let (labels, value) = snapshot.get(PASS_FAILURES_TOTAL).unwrap();
        assert_eq!(labels.get("partition_id"), Some(&"2".to_string()));
        assert_eq!(labels.get("kind"), Some(&"delete".to_string()));
        assert_eq!(value, &DebugValue::Counter(1));

        let (labels, value) = snapshot.get(PASS_DURATION_SECONDS).unwrap();
        assert_eq!(labels.get("partition_id"), Some(&"2".to_string()));
        match value {
            DebugValue::Histogram(values) => assert_eq!(values[0].into_inner(), 0.003f64),
            other => panic!("expected histogram, got {other:?}"),
        }

        let (labels, value) = snapshot.get(OLDEST_IMAGE_AGE_SECONDS).unwrap();
        assert_eq!(labels.get("partition_id"), Some(&"2".to_string()));
        assert_eq!(value, &DebugValue::Gauge(0.008f64.into()));
    }
}
