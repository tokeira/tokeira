//! Process-level metrics recorder setup.
//!
//! Domain crates define and emit their own metrics. This module installs the
//! global Prometheus recorder and emits only process metadata that is common to
//! every Tokeira binary.

use metrics::{counter, gauge};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

use crate::{
    ClusterStatusLabel, DbClassLabel, EmbeddedOperationLabel, EmbeddedStorageModeLabel,
    ErrorClassLabel, LabelCardinality, LabelDescriptor, MetricDescriptor, MetricManifest,
    MetricType, MetricUnit, ObservabilityError, OwnershipOutcomeLabel, ProcessObservabilityConfig,
    SchemaOutcomeLabel,
};

/// Build metadata gauge; value is always `1`.
pub const BUILD_INFO: &str = "tokeira_build_info";
/// Process start timestamp as Unix seconds.
pub const PROCESS_START_TIME_SECONDS: &str = "tokeira_process_metadata_start_time_seconds";
/// Embedded lifecycle attempts, classified only by bounded operational dimensions.
pub const EMBEDDED_LIFECYCLE_OPERATIONS_TOTAL: &str = "tokeira_embedded_lifecycle_operations_total";

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

const STORAGE_MODE_LABEL: LabelDescriptor = LabelDescriptor {
    name: "storage_mode",
    cardinality: LabelCardinality::BoundedEnum,
    allowed_values: &["in_memory", "managed_dsql", "existing_dsql"],
    max_cardinality_hint: Some(3),
    description: "explicit embedded storage mode",
};

const CLUSTER_STATUS_LABEL: LabelDescriptor = LabelDescriptor {
    name: "cluster_status",
    cardinality: LabelCardinality::BoundedEnum,
    allowed_values: &[
        "not_applicable",
        "creating",
        "active",
        "idle",
        "inactive",
        "updating",
        "deleting",
        "deleted",
        "failed",
        "pending_setup",
        "pending_delete",
        "unknown",
    ],
    max_cardinality_hint: Some(12),
    description: "bounded Aurora DSQL control-plane status",
};

const SCHEMA_OUTCOME_LABEL: LabelDescriptor = LabelDescriptor {
    name: "schema_outcome",
    cardinality: LabelCardinality::BoundedEnum,
    allowed_values: &[
        "not_applicable",
        "compatible",
        "initialized",
        "migrated",
        "metadata_backfilled",
        "migration_required",
        "incompatible",
        "failed",
    ],
    max_cardinality_hint: Some(8),
    description: "bounded release/schema compatibility outcome",
};

const OWNERSHIP_OUTCOME_LABEL: LabelDescriptor = LabelDescriptor {
    name: "ownership_outcome",
    cardinality: LabelCardinality::BoundedEnum,
    allowed_values: &[
        "not_applicable",
        "acquired_clean",
        "acquired_expired",
        "released",
        "lost",
        "rejected",
    ],
    max_cardinality_hint: Some(6),
    description: "bounded exclusive embedded-owner outcome",
};

const DATABASE_CLASS_LABEL: LabelDescriptor = LabelDescriptor {
    name: "database_class",
    cardinality: LabelCardinality::BoundedEnum,
    allowed_values: &["control", "commit", "read", "projection", "maintenance"],
    max_cardinality_hint: Some(5),
    description: "bounded DSQL connection budget class",
};

const OPERATION_KIND_LABEL: LabelDescriptor = LabelDescriptor {
    name: "operation_kind",
    cardinality: LabelCardinality::BoundedEnum,
    allowed_values: &[
        "startup",
        "cluster_recovery",
        "schema",
        "ownership",
        "shutdown",
        "destroy_plan",
        "destroy_apply",
    ],
    max_cardinality_hint: Some(7),
    description: "bounded embedded lifecycle operation",
};

const ERROR_CLASS_LABEL: LabelDescriptor = LabelDescriptor {
    name: "error_class",
    cardinality: LabelCardinality::BoundedEnum,
    allowed_values: &[
        "none",
        "configuration",
        "descriptor",
        "access_denied",
        "quota",
        "retryable",
        "identity",
        "status",
        "storage",
        "schema",
        "ownership",
        "deadline",
        "internal",
    ],
    max_cardinality_hint: Some(13),
    description: "redacted bounded failure class",
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

/// Metrics emitted by the embeddable library without installing a recorder.
pub static EMBEDDED_METRIC_MANIFEST: MetricManifest = MetricManifest {
    crate_name: "tokeira-observability-embedded",
    metrics: &[MetricDescriptor {
        name: EMBEDDED_LIFECYCLE_OPERATIONS_TOTAL,
        metric_type: MetricType::Counter,
        unit: MetricUnit::Count,
        description: "Embedded lifecycle attempts by bounded operational outcome.",
        labels: &[
            STORAGE_MODE_LABEL,
            CLUSTER_STATUS_LABEL,
            SCHEMA_OUTCOME_LABEL,
            OWNERSHIP_OUTCOME_LABEL,
            DATABASE_CLASS_LABEL,
            OPERATION_KIND_LABEL,
            ERROR_CLASS_LABEL,
        ],
    }],
};

/// Emit one embedded lifecycle observation through the host's current recorder.
///
/// This function performs no recorder installation and accepts only enums, so
/// execution identifiers, content, and credentials cannot become metric labels.
#[allow(clippy::too_many_arguments)]
pub fn record_embedded_lifecycle(
    storage_mode: EmbeddedStorageModeLabel,
    cluster_status: ClusterStatusLabel,
    schema_outcome: SchemaOutcomeLabel,
    ownership_outcome: OwnershipOutcomeLabel,
    database_class: DbClassLabel,
    operation_kind: EmbeddedOperationLabel,
    error_class: ErrorClassLabel,
) {
    counter!(
        EMBEDDED_LIFECYCLE_OPERATIONS_TOTAL,
        "storage_mode" => storage_mode.as_str(),
        "cluster_status" => cluster_status.as_str(),
        "schema_outcome" => schema_outcome.as_str(),
        "ownership_outcome" => ownership_outcome.as_str(),
        "database_class" => database_class.as_str(),
        "operation_kind" => operation_kind.as_str(),
        "error_class" => error_class.as_str(),
    )
    .increment(1);
}

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
    use proptest::prelude::*;

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
    fn embedded_metric_manifest_is_valid() {
        validate_manifest(&EMBEDDED_METRIC_MANIFEST).unwrap();
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        // Feature: managed-embedded-dsql, Property 17: metric dimensions stay bounded
        #[test]
        fn embedded_metric_dimensions_stay_bounded(
            workflow_ids in prop::collection::vec("workflow-[a-zA-Z0-9]{1,32}", 0..24),
            run_ids in prop::collection::vec("run-[a-zA-Z0-9]{1,32}", 0..24),
            activity_ids in prop::collection::vec("activity-[a-zA-Z0-9]{1,32}", 0..24),
            prompts in prop::collection::vec("prompt-[a-zA-Z0-9]{1,32}", 0..24),
            tool_inputs in prop::collection::vec("tool-[a-zA-Z0-9]{1,32}", 0..24),
        ) {
            let recorder = DebuggingRecorder::new();
            let repetitions = workflow_ids.len()
                .max(run_ids.len())
                .max(activity_ids.len())
                .max(prompts.len())
                .max(tool_inputs.len())
                .max(1);
            with_local_recorder(&recorder, || {
                for _ in 0..repetitions {
                    record_embedded_lifecycle(
                        EmbeddedStorageModeLabel::ManagedDsql,
                        ClusterStatusLabel::Active,
                        SchemaOutcomeLabel::Compatible,
                        OwnershipOutcomeLabel::AcquiredClean,
                        DbClassLabel::Control,
                        EmbeddedOperationLabel::Startup,
                        ErrorClassLabel::None,
                    );
                }
            });

            let snapshot = recorder.snapshotter().snapshot().into_vec();
            let metric = snapshot.iter()
                .find(|(key, _, _, _)| key.key().name() == EMBEDDED_LIFECYCLE_OPERATIONS_TOTAL)
                .expect("embedded metric recorded");
            let labels = metric.0.key().labels()
                .map(|label| (label.key(), label.value()))
                .collect::<std::collections::HashMap<_, _>>();
            let descriptor = &EMBEDDED_METRIC_MANIFEST.metrics[0];
            prop_assert_eq!(labels.len(), descriptor.labels.len());
            for label in descriptor.labels {
                let value = labels.get(label.name).expect("manifest label emitted");
                prop_assert!(label.allowed_values.contains(value));
            }
            let rendered = format!("{labels:?}");
            for canary in workflow_ids.iter()
                .chain(run_ids.iter())
                .chain(activity_ids.iter())
                .chain(prompts.iter())
                .chain(tool_inputs.iter())
            {
                prop_assert!(!rendered.contains(canary));
            }
        }
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
