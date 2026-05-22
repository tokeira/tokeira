//! Process-level metrics recorder setup.
//!
//! Domain crates define and emit their own metrics. This module installs the
//! global Prometheus recorder and emits only process metadata that is common to
//! every Tokeira binary.

use metrics::gauge;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

use crate::{
    LabelCardinality, LabelDescriptor, MetricDescriptor, MetricManifest, MetricType, MetricUnit,
    ObservabilityError, ProcessObservabilityConfig,
};

/// Build metadata gauge; value is always `1`.
pub const BUILD_INFO: &str = "tokeira_build_info";
/// Process start timestamp as Unix seconds.
pub const PROCESS_START_TIME_SECONDS: &str = "tokeira_process_metadata_start_time_seconds";

const SERVICE_LABEL: LabelDescriptor = LabelDescriptor {
    name: "service",
    cardinality: LabelCardinality::BoundedEnum,
    allowed_values: &[
        "tokeirad",
        "tokeira-controller",
        "tokeira-autoscaler",
        "alloy",
        "mimir",
        "loki",
        "grafana",
    ],
    max_cardinality_hint: Some(7),
    description: "process service name",
};

const CLUSTER_LABEL: LabelDescriptor = LabelDescriptor {
    name: "cluster",
    cardinality: LabelCardinality::ConfigurationBounded,
    allowed_values: &[],
    max_cardinality_hint: Some(128),
    description: "operator-defined cluster name",
};

const DEPLOYMENT_LABEL: LabelDescriptor = LabelDescriptor {
    name: "deployment",
    cardinality: LabelCardinality::ConfigurationBounded,
    allowed_values: &[],
    max_cardinality_hint: Some(128),
    description: "operator-defined deployment name",
};

const VERSION_LABEL: LabelDescriptor = LabelDescriptor {
    name: "version",
    cardinality: LabelCardinality::ConfigurationBounded,
    allowed_values: &[],
    max_cardinality_hint: Some(128),
    description: "build version",
};

const COMMIT_LABEL: LabelDescriptor = LabelDescriptor {
    name: "commit",
    cardinality: LabelCardinality::ConfigurationBounded,
    allowed_values: &[],
    max_cardinality_hint: Some(1024),
    description: "git commit for the running build",
};

const RUSTC_VERSION_LABEL: LabelDescriptor = LabelDescriptor {
    name: "rustc_version",
    cardinality: LabelCardinality::ConfigurationBounded,
    allowed_values: &[],
    max_cardinality_hint: Some(32),
    description: "compiler version for the running build",
};

pub static PROCESS_METRIC_MANIFEST: MetricManifest = MetricManifest {
    crate_name: "tokeira-observability",
    metrics: &[
        MetricDescriptor {
            name: BUILD_INFO,
            metric_type: MetricType::Gauge,
            unit: MetricUnit::Info,
            description: "Build metadata for the running Tokeira process.",
            labels: &[
                SERVICE_LABEL,
                CLUSTER_LABEL,
                DEPLOYMENT_LABEL,
                VERSION_LABEL,
                COMMIT_LABEL,
                RUSTC_VERSION_LABEL,
            ],
        },
        MetricDescriptor {
            name: PROCESS_START_TIME_SECONDS,
            metric_type: MetricType::Gauge,
            unit: MetricUnit::Seconds,
            description: "Unix timestamp at which the Tokeira process started.",
            labels: &[SERVICE_LABEL, CLUSTER_LABEL, DEPLOYMENT_LABEL],
        },
    ],
};

/// Install the global Prometheus recorder when enabled.
///
/// The manifest slice is accepted at this boundary to keep the call shape close
/// to `install_observability`; validation happens before this function is
/// called. The Prometheus recorder does not need explicit registration for each
/// metric and will render observed metrics dynamically.
pub fn install_prometheus_recorder(
    config: &ProcessObservabilityConfig,
    _manifests: &[&'static MetricManifest],
) -> Result<Option<PrometheusHandle>, ObservabilityError> {
    if !config.metrics_enabled {
        return Ok(None);
    }

    let handle = PrometheusBuilder::new()
        .install_recorder()
        .map_err(|error| ObservabilityError::RecorderInstall(error.to_string()))?;

    record_process_metadata(config);

    Ok(Some(handle))
}

/// Emit process metadata gauges once after recorder installation.
pub fn record_process_metadata(config: &ProcessObservabilityConfig) {
    let service = config.service_name.to_string();
    gauge!(
        BUILD_INFO,
        "version" => env!("CARGO_PKG_VERSION").to_string(),
        "commit" => option_env!("TOKEIRA_GIT_COMMIT").unwrap_or("unknown").to_string(),
        "rustc_version" => option_env!("RUSTC_VERSION").unwrap_or("unknown").to_string(),
        "service" => service.clone(),
        "cluster" => config.cluster_name.clone(),
        "deployment" => config.deployment_name.clone(),
    )
    .set(1.0);

    let start_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    gauge!(
        PROCESS_START_TIME_SECONDS,
        "service" => service,
        "cluster" => config.cluster_name.clone(),
        "deployment" => config.deployment_name.clone(),
    )
    .set(start_time);
}

#[cfg(test)]
mod tests {
    use metrics::with_local_recorder;
    use metrics_util::debugging::DebuggingRecorder;

    use crate::{LogFormat, OtlpMetricsConfig, ServiceName, TraceExportConfig, validate_manifest};

    use super::*;

    fn test_config() -> ProcessObservabilityConfig {
        ProcessObservabilityConfig {
            service_name: ServiceName::Tokeirad,
            cluster_name: "test-cluster".to_string(),
            deployment_name: "test-deployment".to_string(),
            node_id: None,
            task_id: None,
            metrics_enabled: true,
            metrics_addr: "127.0.0.1:0".parse().unwrap(),
            log_format: LogFormat::Text,
            log_filter: "info".to_string(),
            otlp_metrics: OtlpMetricsConfig::default(),
            tracing: TraceExportConfig::default(),
            shutdown_flush_timeout: std::time::Duration::from_secs(1),
            redacted_config: None,
        }
    }

    #[test]
    fn process_metric_manifest_is_valid() {
        validate_manifest(&PROCESS_METRIC_MANIFEST).unwrap();
    }

    #[test]
    fn metadata_metrics_include_process_labels() {
        let recorder = DebuggingRecorder::new();
        let config = test_config();

        with_local_recorder(&recorder, || {
            record_process_metadata(&config);
        });

        let snapshot = recorder.snapshotter().snapshot().into_vec();
        let build_info = snapshot
            .iter()
            .find(|(key, _, _, _)| key.key().name() == BUILD_INFO)
            .expect("build info metric should be recorded");
        let labels = build_info
            .0
            .key()
            .labels()
            .map(|label| (label.key(), label.value()))
            .collect::<Vec<_>>();

        assert!(labels.contains(&("service", "tokeirad")));
        assert!(labels.contains(&("cluster", "test-cluster")));
        assert!(labels.contains(&("deployment", "test-deployment")));
        assert!(labels.iter().any(|(key, _)| *key == "version"));
        assert!(labels.iter().any(|(key, _)| *key == "commit"));
        assert!(labels.iter().any(|(key, _)| *key == "rustc_version"));
    }
}
