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
/// Authorization admission latency by public API and outcome.
pub const AUTHORIZATION_DURATION_SECONDS: &str = "tokeira_edge_authorization_duration_seconds";
/// Intentional policy denials by public API.
pub const AUTHORIZATION_DENIED_TOTAL: &str = "tokeira_edge_authorization_denied_total";
/// Authentication or authorizer implementation failures by stage.
pub const AUTHORIZATION_ERROR_TOTAL: &str = "tokeira_edge_authorization_error_total";
const EDGE_SERVICE_LABEL: &str = "edge";

// Nexus operational metrics owned by the edge. Names are tokeira-prefixed; the conformance
// metrics bridge renames them to Temporal's (`nexus_completion_requests`,
// `nexus_task_requests`, …) and passes the labels through. These mirror v1.31.0's Nexus
// metric surface (`common/metrics/metric_defs.go`, `chasm/lib/nexusoperation/metrics.go`):
//
// - completion (inbound `/nexus/callback` handler) → `nexus_completion_*`
// - task dispatch (`PollNexusTaskQueue`) → `nexus_task_requests`
//
// The outbound metric (`nexus_outbound_requests` / `_latency`) is owned by `tokeira-runtime`
// (the history-service analogue), since it is recorded both at the runtime's External-endpoint
// `start_operation` and — across the edge→runtime dependency — at the worker-response handlers.
//
// `outcome` is a bounded enum (success / error_bad_request / error_not_found /
// error_internal for completion). `namespace` is the only otherwise-dynamic label and is a
// sanctioned key.

/// Inbound Nexus completion requests, by `outcome` (maps to `nexus_completion_requests`).
pub const NEXUS_COMPLETION_REQUESTS_TOTAL: &str = "tokeira_edge_nexus_completion_requests_total";
/// Inbound Nexus completion handler latency (maps to `nexus_completion_latency`).
pub const NEXUS_COMPLETION_LATENCY_SECONDS: &str = "tokeira_edge_nexus_completion_latency_seconds";
/// Inbound Nexus completion requests rejected during pre-processing (maps to
/// `nexus_completion_request_preprocess_errors`): a malformed token/state/body rejected
/// before the operation is resolved.
pub const NEXUS_COMPLETION_PREPROCESS_ERRORS_TOTAL: &str =
    "tokeira_edge_nexus_completion_request_preprocess_errors_total";
/// Nexus task dispatch requests served to workers (maps to `nexus_task_requests`).
pub const NEXUS_TASK_REQUESTS_TOTAL: &str = "tokeira_edge_nexus_task_requests_total";
/// Normal service telemetry for caller-facing Nexus HTTP methods (maps to `service_requests`).
pub const SERVICE_REQUESTS_TOTAL: &str = "tokeira_edge_service_requests_total";
/// Caller-facing Nexus HTTP requests after route resolution (maps to `nexus_requests`).
pub const NEXUS_REQUESTS_TOTAL: &str = "tokeira_edge_nexus_requests_total";
/// Caller-facing Nexus HTTP request latency (maps to `nexus_latency`).
pub const NEXUS_LATENCY_SECONDS: &str = "tokeira_edge_nexus_latency_seconds";
/// Caller-facing Nexus HTTP requests rejected before dispatch (maps to
/// `nexus_request_preprocess_errors`).
pub const NEXUS_REQUEST_PREPROCESS_ERRORS_TOTAL: &str =
    "tokeira_edge_nexus_request_preprocess_errors_total";

pub const METRIC_NAMES: &[(&str, MetricType)] = &[
    (GRPC_REQUEST_TOTAL, MetricType::Counter),
    (GRPC_REQUEST_DURATION_SECONDS, MetricType::DurationHistogram),
    (GRPC_ERROR_TOTAL, MetricType::Counter),
    (GRPC_ACTIVE_REQUESTS, MetricType::Gauge),
    (
        AUTHORIZATION_DURATION_SECONDS,
        MetricType::DurationHistogram,
    ),
    (AUTHORIZATION_DENIED_TOTAL, MetricType::Counter),
    (AUTHORIZATION_ERROR_TOTAL, MetricType::Counter),
    (NEXUS_COMPLETION_REQUESTS_TOTAL, MetricType::Counter),
    (
        NEXUS_COMPLETION_LATENCY_SECONDS,
        MetricType::DurationHistogram,
    ),
    (
        NEXUS_COMPLETION_PREPROCESS_ERRORS_TOTAL,
        MetricType::Counter,
    ),
    (NEXUS_TASK_REQUESTS_TOTAL, MetricType::Counter),
    (SERVICE_REQUESTS_TOTAL, MetricType::Counter),
    (NEXUS_REQUESTS_TOTAL, MetricType::Counter),
    (NEXUS_LATENCY_SECONDS, MetricType::DurationHistogram),
    (NEXUS_REQUEST_PREPROCESS_ERRORS_TOTAL, MetricType::Counter),
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

/// Record the complete authenticate-plus-authorize admission duration.
pub fn record_authorization_duration(api_name: &str, outcome: &str, duration: std::time::Duration) {
    histogram!(
        AUTHORIZATION_DURATION_SECONDS,
        "api_name" => api_name.to_owned(),
        "outcome" => outcome.to_owned(),
    )
    .record(duration.as_secs_f64());
}

/// Record one intentional policy denial.
pub fn record_authorization_denied(api_name: &str) {
    counter!(AUTHORIZATION_DENIED_TOTAL, "api_name" => api_name.to_owned()).increment(1);
}

/// Record an authentication or authorizer implementation failure.
pub fn record_authorization_error(api_name: &str, stage: &str) {
    counter!(
        AUTHORIZATION_ERROR_TOTAL,
        "api_name" => api_name.to_owned(),
        "stage" => stage.to_owned(),
    )
    .increment(1);
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

/// Record one Nexus worker RPC at admission.
///
/// Temporal v1.31.0 records this for poll and respond RPCs with the worker's
/// client name and an internal-queue discriminator, independently of whether a
/// long poll eventually finds work (`service/matching/handler.go:165-176,523-572
/// @ v1.31.0`).
pub fn record_nexus_task_request(
    namespace: &str,
    operation: &str,
    client_name: &str,
    is_internal: bool,
) {
    counter!(
        NEXUS_TASK_REQUESTS_TOTAL,
        "namespace" => namespace.to_string(),
        "operation" => operation.to_string(),
        "client_name" => client_name.to_string(),
        "is_internal" => is_internal.to_string(),
    )
    .increment(1);
}

/// Record one caller-facing Nexus operation in the edge's normal service
/// telemetry surface.
pub fn record_service_request(namespace: &str, operation: &str) {
    counter!(
        SERVICE_REQUESTS_TOTAL,
        "namespace" => namespace.to_owned(),
        "operation" => operation.to_owned(),
    )
    .increment(1);
}

/// Record one terminal caller-facing Nexus HTTP outcome.
///
/// Temporal v1.31.0 attaches the resolved namespace, method and endpoint even
/// when authorization rejects the call (`service/frontend/nexus_handler.go`).
pub fn record_nexus_request(namespace: &str, method: &str, outcome: &str, nexus_endpoint: &str) {
    counter!(
        NEXUS_REQUESTS_TOTAL,
        "namespace" => namespace.to_owned(),
        "method" => method.to_owned(),
        "outcome" => outcome.to_owned(),
        "nexus_endpoint" => nexus_endpoint.to_owned(),
    )
    .increment(1);
}

/// Record caller-facing Nexus HTTP latency with the same dimensions as the
/// terminal counter so an operator can correlate rate and duration.
pub fn record_nexus_latency(
    namespace: &str,
    method: &str,
    outcome: &str,
    nexus_endpoint: &str,
    duration: std::time::Duration,
) {
    histogram!(
        NEXUS_LATENCY_SECONDS,
        "namespace" => namespace.to_owned(),
        "method" => method.to_owned(),
        "outcome" => outcome.to_owned(),
        "nexus_endpoint" => nexus_endpoint.to_owned(),
    )
    .record(duration.as_secs_f64());
}

/// Record a caller-facing Nexus HTTP request rejected during route or request
/// preprocessing, before a task becomes visible to a worker.
pub fn record_nexus_request_preprocess_error() {
    counter!(NEXUS_REQUEST_PREPROCESS_ERRORS_TOTAL).increment(1);
}

#[derive(Debug)]
pub struct GrpcActiveRequestGuard {
    method: String,
}

impl Drop for GrpcActiveRequestGuard {
    fn drop(&mut self) {
        let mut active = active_requests()
            .lock()
            .expect("active_requests lock poisoned");
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
    let mut active = active_requests()
        .lock()
        .expect("active_requests lock poisoned");
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

    use proptest::prelude::*;

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
    fn authorization_metrics_use_only_bounded_api_outcome_and_stage_labels() {
        let recorder = DebuggingRecorder::new();

        with_local_recorder(&recorder, || {
            record_authorization_duration(
                "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution",
                "denied",
                std::time::Duration::from_millis(2),
            );
            record_authorization_denied(
                "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution",
            );
            record_authorization_error(
                "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution",
                "authorizer",
            );
        });

        let snapshot = snapshot_map(&recorder);
        let expected_api =
            "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution".to_owned();

        let (labels, value) = snapshot
            .get(AUTHORIZATION_DURATION_SECONDS)
            .expect("authorization duration");
        assert_eq!(labels.get("api_name"), Some(&expected_api));
        assert_eq!(labels.get("outcome"), Some(&"denied".to_owned()));
        assert_eq!(labels.len(), 2);
        assert!(matches!(value, DebugValue::Histogram(values) if values.len() == 1));

        let (labels, value) = snapshot
            .get(AUTHORIZATION_DENIED_TOTAL)
            .expect("authorization denial");
        assert_eq!(labels.get("api_name"), Some(&expected_api));
        assert_eq!(labels.len(), 1);
        assert_eq!(value, &DebugValue::Counter(1));

        let (labels, value) = snapshot
            .get(AUTHORIZATION_ERROR_TOTAL)
            .expect("authorization error");
        assert_eq!(labels.get("api_name"), Some(&expected_api));
        assert_eq!(labels.get("stage"), Some(&"authorizer".to_owned()));
        assert_eq!(labels.len(), 2);
        assert_eq!(value, &DebugValue::Counter(1));
    }

    #[test]
    fn nexus_helpers_emit_expected_metrics_and_labels() {
        let recorder = DebuggingRecorder::new();

        with_local_recorder(&recorder, || {
            record_nexus_completion_request("default", "success");
            record_nexus_completion_latency("default", std::time::Duration::from_millis(7));
            record_nexus_completion_preprocess_error("default");
            record_nexus_task_request("default", "PollNexusTaskQueue", "temporal-go", false);
            record_service_request("default", "StartNexusOperation");
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

        let (labels, value) = snapshot.get(NEXUS_TASK_REQUESTS_TOTAL).unwrap();
        assert_eq!(labels.get("namespace"), Some(&"default".to_string()));
        assert_eq!(
            labels.get("operation"),
            Some(&"PollNexusTaskQueue".to_string())
        );
        assert_eq!(labels.get("client_name"), Some(&"temporal-go".to_string()));
        assert_eq!(labels.get("is_internal"), Some(&"false".to_string()));
        assert_eq!(value, &DebugValue::Counter(1));

        let (labels, value) = snapshot.get(SERVICE_REQUESTS_TOTAL).unwrap();
        assert_eq!(labels.get("namespace"), Some(&"default".to_string()));
        assert_eq!(
            labels.get("operation"),
            Some(&"StartNexusOperation".to_string())
        );
        assert_eq!(value, &DebugValue::Counter(1));
    }

    // Feature: edge-nexus-http-dispatch, Property 7: one terminal caller-facing
    // outcome produces exactly one counter sample and one latency observation
    // with the same bounded dimensions; preprocessing remains a separate counter.
    proptest! {
        #[test]
        fn property_nexus_http_metrics_are_emitted_once(
            namespace in "[a-z0-9_-]{1,24}",
            method in prop_oneof![Just("StartNexusOperation"), Just("CancelNexusOperation")],
            outcome in prop_oneof![Just("success"), Just("unauthorized"), Just("internal_error")],
            endpoint in "[a-z0-9_-]{1,24}",
        ) {
            let recorder = DebuggingRecorder::new();
            with_local_recorder(&recorder, || {
                record_nexus_request(&namespace, method, outcome, &endpoint);
                record_nexus_latency(
                    &namespace,
                    method,
                    outcome,
                    &endpoint,
                    std::time::Duration::from_millis(3),
                );
                record_service_request(&namespace, method);
                record_nexus_request_preprocess_error();
            });
            let snapshot = snapshot_map(&recorder);

            let (labels, value) = snapshot
                .get(NEXUS_REQUESTS_TOTAL)
                .expect("terminal counter");
            prop_assert_eq!(value, &DebugValue::Counter(1));
            prop_assert_eq!(labels.get("namespace"), Some(&namespace));
            prop_assert_eq!(labels.get("method").map(String::as_str), Some(method));
            prop_assert_eq!(labels.get("outcome").map(String::as_str), Some(outcome));
            prop_assert_eq!(labels.get("nexus_endpoint"), Some(&endpoint));

            let (_, value) = snapshot
                .get(NEXUS_LATENCY_SECONDS)
                .expect("terminal latency");
            match value {
                DebugValue::Histogram(values) => prop_assert_eq!(values.len(), 1),
                other => prop_assert!(false, "expected histogram, got {other:?}"),
            }
            let (labels, value) = snapshot
                .get(SERVICE_REQUESTS_TOTAL)
                .expect("service request");
            prop_assert_eq!(value, &DebugValue::Counter(1));
            prop_assert_eq!(labels.get("namespace"), Some(&namespace));
            prop_assert_eq!(labels.get("operation").map(String::as_str), Some(method));
            let (labels, value) = snapshot
                .get(NEXUS_REQUEST_PREPROCESS_ERRORS_TOTAL)
                .expect("preprocess counter");
            prop_assert!(labels.is_empty());
            prop_assert_eq!(value, &DebugValue::Counter(1));
        }
    }
}
