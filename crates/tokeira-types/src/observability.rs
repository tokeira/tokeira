//! Shared observability naming and bucket conventions.
//!
//! Library crates record metrics independently, but they should not invent
//! incompatible naming schemes or label keys. This module centralises the
//! cross-crate conventions so metric manifests can be validated in tests.

use thiserror::Error;

/// Histogram bucket boundaries for latency measurements in seconds.
pub const LATENCY_BUCKETS_SECONDS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// Histogram bucket boundaries for size measurements in bytes.
pub const SIZE_BUCKETS_BYTES: &[u64] = &[256, 1024, 4096, 16384, 65536, 262144, 1048576];

/// Standard metric label for workflow namespaces.
pub const LABEL_NAMESPACE: &str = "namespace";
/// Standard metric label for task queue names.
pub const LABEL_TASK_QUEUE: &str = "task_queue";
/// Standard metric label for operation names.
pub const LABEL_OPERATION: &str = "operation";
/// Standard metric label for success/error outcomes.
pub const LABEL_STATUS: &str = "status";

/// Metric families supported by the shared naming validator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetricType {
    Counter,
    Gauge,
    Histogram,
    DurationHistogram,
    SizeHistogram,
}

/// Validation error for metric-name manifests.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum NamingError {
    #[error("metric names must start with `tokeira_`: {0}")]
    InvalidPrefix(String),
    #[error("metric names must include at least four segments after `tokeira_`: {0}")]
    TooFewSegments(String),
    #[error("counter metrics must end with `_total`: {0}")]
    InvalidCounterSuffix(String),
    #[error("duration histograms must end with `_seconds`: {0}")]
    InvalidDurationSuffix(String),
    #[error("size histograms must end with `_bytes`: {0}")]
    InvalidSizeSuffix(String),
}

/// Validate a metric name against the Tokeira naming convention.
pub fn validate_metric_name(name: &str, metric_type: MetricType) -> Result<(), NamingError> {
    if name == "tokeira_build_info" {
        return match metric_type {
            MetricType::Gauge => Ok(()),
            _ => Err(NamingError::TooFewSegments(name.to_string())),
        };
    }

    let Some(without_prefix) = name.strip_prefix("tokeira_") else {
        return Err(NamingError::InvalidPrefix(name.to_string()));
    };

    if without_prefix.split('_').count() < 4 {
        return Err(NamingError::TooFewSegments(name.to_string()));
    }

    match metric_type {
        MetricType::Counter if !name.ends_with("_total") => {
            Err(NamingError::InvalidCounterSuffix(name.to_string()))
        }
        MetricType::DurationHistogram if !name.ends_with("_seconds") => {
            Err(NamingError::InvalidDurationSuffix(name.to_string()))
        }
        MetricType::SizeHistogram if !name.ends_with("_bytes") => {
            Err(NamingError::InvalidSizeSuffix(name.to_string()))
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn accepts_valid_names() {
        assert!(
            validate_metric_name("tokeira_runtime_broker_publish_total", MetricType::Counter)
                .is_ok()
        );
        assert!(
            validate_metric_name(
                "tokeira_edge_grpc_request_duration_seconds",
                MetricType::DurationHistogram
            )
            .is_ok()
        );
        assert!(
            validate_metric_name("tokeira_projection_worker_lag_records", MetricType::Gauge)
                .is_ok()
        );
        assert!(validate_metric_name("tokeira_build_info", MetricType::Gauge).is_ok());
    }

    #[test]
    fn rejects_invalid_names() {
        assert!(matches!(
            validate_metric_name("runtime_broker_publish_total", MetricType::Counter),
            Err(NamingError::InvalidPrefix(_))
        ));
        assert!(matches!(
            validate_metric_name("tokeira_runtime_total", MetricType::Counter),
            Err(NamingError::TooFewSegments(_))
        ));
        assert!(matches!(
            validate_metric_name(
                "tokeira_runtime_broker_publish_seconds",
                MetricType::Counter
            ),
            Err(NamingError::InvalidCounterSuffix(_))
        ));
    }

    #[test]
    fn histogram_buckets_match_documented_values() {
        assert_eq!(
            LATENCY_BUCKETS_SECONDS,
            &[
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0
            ]
        );
        assert_eq!(
            SIZE_BUCKETS_BYTES,
            &[256, 1024, 4096, 16384, 65536, 262144, 1048576]
        );
    }

    #[test]
    fn label_constants_match_shared_contract() {
        assert_eq!(LABEL_NAMESPACE, "namespace");
        assert_eq!(LABEL_TASK_QUEUE, "task_queue");
        assert_eq!(LABEL_OPERATION, "operation");
        assert_eq!(LABEL_STATUS, "status");
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn property_metric_name_validation(
            crate_name in "[a-z]{1,8}",
            subsystem in "[a-z]{1,8}",
            metric in "[a-z]{1,12}",
            unit in "[a-z]{1,8}",
            extra in prop::collection::vec("[a-z]{1,8}", 0..3),
            valid_prefix in any::<bool>(),
            metric_type in prop_oneof![
                Just(MetricType::Counter),
                Just(MetricType::Gauge),
                Just(MetricType::Histogram),
                Just(MetricType::DurationHistogram),
                Just(MetricType::SizeHistogram),
            ],
        ) {
            let mut segments = vec![crate_name, subsystem, metric];
            segments.extend(extra);
            let suffix = match metric_type {
                MetricType::Counter => "total".to_string(),
                MetricType::Gauge => unit,
                MetricType::Histogram => unit,
                MetricType::DurationHistogram => "seconds".to_string(),
                MetricType::SizeHistogram => "bytes".to_string(),
            };
            segments.push(suffix);
            let prefix = if valid_prefix { "tokeira" } else { "broken" };
            let name = format!("{}_{}", prefix, segments.join("_"));

            let result = validate_metric_name(&name, metric_type);
            let enough_segments = segments.len() >= 4;
            let expected_ok = valid_prefix && enough_segments;

            prop_assert_eq!(result.is_ok(), expected_ok);
        }
    }
}
