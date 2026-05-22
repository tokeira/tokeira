//! Edge metric definitions and recording helpers.

use std::{
    collections::HashMap,
    sync::{Mutex, OnceLock},
};

use metrics::{counter, gauge, histogram};
use tokeira_types::MetricType;
#[cfg(test)]
use tokeira_types::validate_metric_name;

pub const GRPC_REQUEST_TOTAL: &str = "tokeira_edge_grpc_request_total";
pub const GRPC_REQUEST_DURATION_SECONDS: &str = "tokeira_edge_grpc_request_duration_seconds";
pub const GRPC_ERROR_TOTAL: &str = "tokeira_edge_grpc_error_total";
pub const GRPC_ACTIVE_REQUESTS: &str = "tokeira_edge_grpc_active_requests";
const EDGE_SERVICE_LABEL: &str = "edge";

pub const METRIC_NAMES: &[(&str, MetricType)] = &[
    (GRPC_REQUEST_TOTAL, MetricType::Counter),
    (GRPC_REQUEST_DURATION_SECONDS, MetricType::DurationHistogram),
    (GRPC_ERROR_TOTAL, MetricType::Counter),
    (GRPC_ACTIVE_REQUESTS, MetricType::Gauge),
];

static ACTIVE_REQUESTS: OnceLock<Mutex<HashMap<String, u64>>> = OnceLock::new();

fn active_requests() -> &'static Mutex<HashMap<String, u64>> {
    ACTIVE_REQUESTS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn record_grpc_request(method: &str, namespace: &str, status: &str) {
    counter!(
        GRPC_REQUEST_TOTAL,
        "service" => EDGE_SERVICE_LABEL,
        "method" => method.to_string(),
        "namespace" => namespace.to_string(),
        "status" => status.to_string(),
    )
    .increment(1);
}

pub fn record_grpc_request_duration(method: &str, namespace: &str, duration: std::time::Duration) {
    histogram!(
        GRPC_REQUEST_DURATION_SECONDS,
        "service" => EDGE_SERVICE_LABEL,
        "method" => method.to_string(),
        "namespace" => namespace.to_string(),
    )
    .record(duration.as_secs_f64());
}

pub fn record_grpc_error(method: &str, namespace: &str, error_code: &str) {
    counter!(
        GRPC_ERROR_TOTAL,
        "method" => method.to_string(),
        "namespace" => namespace.to_string(),
        "error_code" => error_code.to_string(),
    )
    .increment(1);
}

pub fn set_grpc_active_requests(method: &str, value: f64) {
    gauge!(GRPC_ACTIVE_REQUESTS, "method" => method.to_string()).set(value);
}

pub struct GrpcActiveRequestGuard {
    method: String,
}

impl Drop for GrpcActiveRequestGuard {
    fn drop(&mut self) {
        let mut active = active_requests().lock().unwrap();
        let next = active
            .get(&self.method)
            .copied()
            .unwrap_or(1)
            .saturating_sub(1);
        if next == 0 {
            active.remove(&self.method);
        } else {
            active.insert(self.method.clone(), next);
        }
        set_grpc_active_requests(&self.method, next as f64);
    }
}

pub fn track_grpc_active_request(method: &str) -> GrpcActiveRequestGuard {
    let mut active = active_requests().lock().unwrap();
    let next = active.get(method).copied().unwrap_or(0) + 1;
    active.insert(method.to_string(), next);
    set_grpc_active_requests(method, next as f64);
    GrpcActiveRequestGuard {
        method: method.to_string(),
    }
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
            record_grpc_request("StartWorkflowExecution", "default", "ok");
            record_grpc_request_duration(
                "StartWorkflowExecution",
                "default",
                std::time::Duration::from_millis(12),
            );
            record_grpc_error("StartWorkflowExecution", "default", "NotFound");
            set_grpc_active_requests("StartWorkflowExecution", 3.0);
        });

        let snapshot = snapshot_map(&recorder);

        let (labels, value) = snapshot.get(GRPC_REQUEST_TOTAL).unwrap();
        assert_eq!(labels.get("service"), Some(&"edge".to_string()));
        assert_eq!(
            labels.get("method"),
            Some(&"StartWorkflowExecution".to_string())
        );
        assert_eq!(labels.get("namespace"), Some(&"default".to_string()));
        assert_eq!(labels.get("status"), Some(&"ok".to_string()));
        assert!(!labels.contains_key("workflow_id"));
        assert!(!labels.contains_key("run_id"));
        assert_eq!(value, &DebugValue::Counter(1));

        let (labels, value) = snapshot.get(GRPC_REQUEST_DURATION_SECONDS).unwrap();
        assert_eq!(labels.get("service"), Some(&"edge".to_string()));
        assert_eq!(
            labels.get("method"),
            Some(&"StartWorkflowExecution".to_string())
        );
        assert_eq!(labels.get("namespace"), Some(&"default".to_string()));
        assert!(!labels.contains_key("workflow_id"));
        assert!(!labels.contains_key("run_id"));
        match value {
            DebugValue::Histogram(values) => {
                assert_eq!(values.len(), 1);
                assert_eq!(values[0].into_inner(), 0.012f64);
            }
            other => panic!("expected histogram, got {other:?}"),
        }

        let (labels, value) = snapshot.get(GRPC_ERROR_TOTAL).unwrap();
        assert_eq!(
            labels.get("method"),
            Some(&"StartWorkflowExecution".to_string())
        );
        assert_eq!(labels.get("namespace"), Some(&"default".to_string()));
        assert_eq!(labels.get("error_code"), Some(&"NotFound".to_string()));
        assert_eq!(value, &DebugValue::Counter(1));

        let (labels, value) = snapshot.get(GRPC_ACTIVE_REQUESTS).unwrap();
        assert_eq!(
            labels.get("method"),
            Some(&"StartWorkflowExecution".to_string())
        );
        assert_eq!(value, &DebugValue::Gauge(3.0.into()));
    }
}
