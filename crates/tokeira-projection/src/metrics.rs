//! Projection metric definitions and recording helpers.

use metrics::{counter, gauge, histogram};
use tokeira_types::MetricType;
#[cfg(test)]
use tokeira_types::validate_metric_name;

pub const RECORDS_PROCESSED_TOTAL: &str = "tokeira_projection_records_processed_total";
pub const LAG_RECORDS: &str = "tokeira_projection_worker_lag_records";
pub const SINK_WRITE_DURATION_SECONDS: &str = "tokeira_projection_sink_write_duration_seconds";
pub const SINK_ERROR_TOTAL: &str = "tokeira_projection_sink_error_total";

pub const METRIC_NAMES: &[(&str, MetricType)] = &[
    (RECORDS_PROCESSED_TOTAL, MetricType::Counter),
    (LAG_RECORDS, MetricType::Gauge),
    (SINK_WRITE_DURATION_SECONDS, MetricType::DurationHistogram),
    (SINK_ERROR_TOTAL, MetricType::Counter),
];

pub fn record_records_processed(partition_id: u32, count: usize) {
    counter!(RECORDS_PROCESSED_TOTAL, "partition_id" => partition_id.to_string())
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
    counter!(SINK_ERROR_TOTAL, "partition_id" => partition_id.to_string()).increment(1);
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
        });

        let snapshot = snapshot_map(&recorder);

        let (labels, value) = snapshot.get(RECORDS_PROCESSED_TOTAL).unwrap();
        assert_eq!(labels.get("partition_id"), Some(&"2".to_string()));
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
        assert_eq!(value, &DebugValue::Counter(1));
    }
}
