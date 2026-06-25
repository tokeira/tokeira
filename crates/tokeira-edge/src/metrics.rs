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

// Nexus operational metrics. Names are tokeira-prefixed; the conformance metrics bridge
// renames them to Temporal's (`nexus_completion_requests`, `nexus_outbound_requests`,
// `nexus_task_requests`, …) and passes the labels through. These mirror v1.31.0's Nexus
// metric surface (`common/metrics/metric_defs.go`, `chasm/lib/nexusoperation/metrics.go`):
//
// - completion (inbound `/nexus/callback` handler) → `nexus_completion_*`
// - outbound (caller-side StartOperation resolution) → `nexus_outbound_*`
// - task dispatch (`PollNexusTaskQueue`) → `nexus_task_requests`
//
// `outcome` is a bounded enum (success / error_bad_request / error_not_found /
// error_internal for completion; pending / handler-error:* / operation-unsuccessful:* for
// outbound). `namespace` is the only otherwise-dynamic label and is a sanctioned key.

/// Inbound Nexus completion requests, by `outcome` (maps to `nexus_completion_requests`).
pub const NEXUS_COMPLETION_REQUESTS_TOTAL: &str = "tokeira_edge_nexus_completion_requests_total";
/// Inbound Nexus completion handler latency (maps to `nexus_completion_latency`).
pub const NEXUS_COMPLETION_LATENCY_SECONDS: &str = "tokeira_edge_nexus_completion_latency_seconds";
/// Inbound Nexus completion requests rejected during pre-processing (maps to
/// `nexus_completion_request_preprocess_errors`): a malformed token/state/body rejected
/// before the operation is resolved.
pub const NEXUS_COMPLETION_PREPROCESS_ERRORS_TOTAL: &str =
    "tokeira_edge_nexus_completion_request_preprocess_errors_total";
/// Caller-side outbound Nexus requests, by `method`/`failure_source`/`outcome` (maps to
/// `nexus_outbound_requests`).
pub const NEXUS_OUTBOUND_REQUESTS_TOTAL: &str = "tokeira_edge_nexus_outbound_requests_total";
/// Caller-side outbound Nexus request latency (maps to `nexus_outbound_latency`).
pub const NEXUS_OUTBOUND_LATENCY_SECONDS: &str = "tokeira_edge_nexus_outbound_latency_seconds";
/// Nexus task dispatch requests served to workers (maps to `nexus_task_requests`).
pub const NEXUS_TASK_REQUESTS_TOTAL: &str = "tokeira_edge_nexus_task_requests_total";

pub const METRIC_NAMES: &[(&str, MetricType)] = &[
    (GRPC_REQUEST_TOTAL, MetricType::Counter),
    (GRPC_REQUEST_DURATION_SECONDS, MetricType::DurationHistogram),
    (GRPC_ERROR_TOTAL, MetricType::Counter),
    (GRPC_ACTIVE_REQUESTS, MetricType::Gauge),
    (NEXUS_COMPLETION_REQUESTS_TOTAL, MetricType::Counter),
    (
        NEXUS_COMPLETION_LATENCY_SECONDS,
        MetricType::DurationHistogram,
    ),
    (
        NEXUS_COMPLETION_PREPROCESS_ERRORS_TOTAL,
        MetricType::Counter,
    ),
    (NEXUS_OUTBOUND_REQUESTS_TOTAL, MetricType::Counter),
    (
        NEXUS_OUTBOUND_LATENCY_SECONDS,
        MetricType::DurationHistogram,
    ),
    (NEXUS_TASK_REQUESTS_TOTAL, MetricType::Counter),
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

/// Record one inbound Nexus completion request, tagged by the originator `namespace` and
/// the handler `outcome` (`success` / `error_bad_request` / `error_not_found` /
/// `error_internal`).
pub fn record_nexus_completion_request(namespace: &str, outcome: &str) {
    counter!(
        NEXUS_COMPLETION_REQUESTS_TOTAL,
        "namespace" => namespace.to_string(),
        "outcome" => outcome.to_string(),
    )
    .increment(1);
}

/// Record the wall-clock latency of an inbound Nexus completion request.
pub fn record_nexus_completion_latency(namespace: &str, duration: std::time::Duration) {
    histogram!(NEXUS_COMPLETION_LATENCY_SECONDS, "namespace" => namespace.to_string())
        .record(duration.as_secs_f64());
}

/// Record a Nexus completion request rejected during pre-processing (malformed
/// token/state/body, before the operation is resolved).
pub fn record_nexus_completion_preprocess_error(namespace: &str) {
    counter!(NEXUS_COMPLETION_PREPROCESS_ERRORS_TOTAL, "namespace" => namespace.to_string())
        .increment(1);
}

/// Record one caller-side outbound Nexus request, tagged by `namespace`, the Nexus `method`
/// (`StartOperation` / `CancelOperation`), the `failure_source` (`worker` / `_unknown_`),
/// and the `outcome` (`pending` / `handler-error:*` / `operation-unsuccessful:*`).
pub fn record_nexus_outbound_request(
    namespace: &str,
    method: &str,
    failure_source: &str,
    outcome: &str,
) {
    counter!(
        NEXUS_OUTBOUND_REQUESTS_TOTAL,
        "namespace" => namespace.to_string(),
        "method" => method.to_string(),
        "failure_source" => failure_source.to_string(),
        "outcome" => outcome.to_string(),
    )
    .increment(1);
}

/// Record the wall-clock latency of a caller-side outbound Nexus request.
pub fn record_nexus_outbound_latency(namespace: &str, method: &str, duration: std::time::Duration) {
    histogram!(
        NEXUS_OUTBOUND_LATENCY_SECONDS,
        "namespace" => namespace.to_string(),
        "method" => method.to_string(),
    )
    .record(duration.as_secs_f64());
}

/// Record one Nexus task dispatched to a worker via `PollNexusTaskQueue`, tagged by
/// `namespace` and `outcome` (`dispatched` / `timeout`).
pub fn record_nexus_task_request(namespace: &str, outcome: &str) {
    counter!(
        NEXUS_TASK_REQUESTS_TOTAL,
        "namespace" => namespace.to_string(),
        "outcome" => outcome.to_string(),
    )
    .increment(1);
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

    #[test]
    fn nexus_helpers_emit_expected_metrics_and_labels() {
        let recorder = DebuggingRecorder::new();

        with_local_recorder(&recorder, || {
            record_nexus_completion_request("default", "success");
            record_nexus_completion_latency("default", std::time::Duration::from_millis(7));
            record_nexus_completion_preprocess_error("default");
            record_nexus_outbound_request(
                "default",
                "StartOperation",
                "worker",
                "handler-error:INTERNAL",
            );
            record_nexus_outbound_latency(
                "default",
                "StartOperation",
                std::time::Duration::from_millis(3),
            );
            record_nexus_task_request("default", "dispatched");
        });

        let snapshot = snapshot_map(&recorder);

        let (labels, value) = snapshot.get(NEXUS_COMPLETION_REQUESTS_TOTAL).unwrap();
        assert_eq!(labels.get("namespace"), Some(&"default".to_string()));
        assert_eq!(labels.get("outcome"), Some(&"success".to_string()));
        assert!(!labels.contains_key("workflow_id"));
        assert!(!labels.contains_key("run_id"));
        assert_eq!(value, &DebugValue::Counter(1));

        assert!(snapshot.contains_key(NEXUS_COMPLETION_LATENCY_SECONDS));

        let (_, value) = snapshot
            .get(NEXUS_COMPLETION_PREPROCESS_ERRORS_TOTAL)
            .unwrap();
        assert_eq!(value, &DebugValue::Counter(1));

        let (labels, value) = snapshot.get(NEXUS_OUTBOUND_REQUESTS_TOTAL).unwrap();
        assert_eq!(labels.get("namespace"), Some(&"default".to_string()));
        assert_eq!(labels.get("method"), Some(&"StartOperation".to_string()));
        assert_eq!(labels.get("failure_source"), Some(&"worker".to_string()));
        assert_eq!(
            labels.get("outcome"),
            Some(&"handler-error:INTERNAL".to_string())
        );
        assert_eq!(value, &DebugValue::Counter(1));

        assert!(snapshot.contains_key(NEXUS_OUTBOUND_LATENCY_SECONDS));

        let (labels, value) = snapshot.get(NEXUS_TASK_REQUESTS_TOTAL).unwrap();
        assert_eq!(labels.get("namespace"), Some(&"default".to_string()));
        assert_eq!(labels.get("outcome"), Some(&"dispatched".to_string()));
        assert_eq!(value, &DebugValue::Counter(1));
    }
}
