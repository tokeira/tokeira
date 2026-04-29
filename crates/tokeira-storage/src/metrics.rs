//! Storage metric definitions and recording helpers.

use metrics::{counter, histogram};
use tokeira_types::MetricType;
#[cfg(test)]
use tokeira_types::validate_metric_name;

pub const COMMIT_TRANSITION_DURATION_SECONDS: &str =
    "tokeira_storage_commit_transition_duration_seconds";
pub const LOAD_RUN_DURATION_SECONDS: &str = "tokeira_storage_load_run_duration_seconds";
pub const READ_HISTORY_DURATION_SECONDS: &str =
    "tokeira_storage_read_history_duration_seconds";
pub const OPERATION_TOTAL: &str = "tokeira_storage_repository_operation_total";

pub const METRIC_NAMES: &[(&str, MetricType)] = &[
    (
        COMMIT_TRANSITION_DURATION_SECONDS,
        MetricType::DurationHistogram,
    ),
    (LOAD_RUN_DURATION_SECONDS, MetricType::DurationHistogram),
    (READ_HISTORY_DURATION_SECONDS, MetricType::DurationHistogram),
    (OPERATION_TOTAL, MetricType::Counter),
];

pub fn record_commit_transition_duration(
    namespace: Option<String>,
    outcome: &'static str,
    duration: std::time::Duration,
) {
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
    counter!(OPERATION_TOTAL, "operation" => operation, "outcome" => outcome)
        .increment(1);
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
    }
}
