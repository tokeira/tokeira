//! Shared production observability primitives for Tokeira processes.
//!
//! This crate deliberately sits above domain crates and below binary crates. It
//! owns process-level installation concerns that must be consistent everywhere:
//! metric manifest validation, bounded metric label vocabularies, structured
//! logging, trace export setup, readiness reporting, redaction, and local
//! observability HTTP endpoints.
//!
//! Domain crates still own their metric recording helpers and metric manifests.
//! That boundary matters: storage, runtime, projection, edge, controller, and
//! autoscaler code can record domain-specific signals without depending on a
//! process bootstrap layer, while every binary gets one shared installation path
//! for global recorders and subscribers.
//!
//! The `metrics` recorder and `tracing` subscriber are process-global. They can
//! only be installed once, so `install_observability` validates everything it can
//! before taking the global installation reservation. If installation fails after
//! the reservation is taken, the reservation is rolled back so tests and
//! embedding binaries do not end up in a permanently poisoned process.

pub mod config;
pub mod http;
pub mod labels;
pub mod logging;
pub mod manifest;
pub mod metrics;
pub mod readiness;
pub mod redaction;
pub mod shutdown;
pub mod testing;
pub mod tracing;

use std::sync::atomic::{AtomicBool, Ordering};

use thiserror::Error;

pub use config::{
    LogFormat, ObservabilityRuntime, OtlpMetricsConfig, OtlpProtocol, ProcessObservabilityConfig,
    ServiceName, TraceExportConfig,
};
pub use http::{ConfigSnapshot, ObservabilityHttpState, spawn_observability_server};
pub use labels::{
    AutoscalerLoopLabel, BoundedLabel, ClusterStatusLabel, ControllerCasOutcomeLabel, DbClassLabel,
    EmbeddedOperationLabel, EmbeddedStorageModeLabel, ErrorClassLabel, NominationOutcomeLabel,
    NotShardOwnerOperationLabel, OutcomeLabel, OwnershipOutcomeLabel, ProjectionErrorKindLabel,
    ProjectionOutcomeLabel, QueryBufferOutcomeLabel, QueryDispatchOutcomeLabel,
    QueryDispatchPathLabel, RetryOutcomeLabel, ScalingDirectionLabel, SchemaOutcomeLabel,
    ServiceLabel, StorageOperationLabel, bounded_metric_label,
};
pub use manifest::{
    FORBIDDEN_METRIC_LABEL_NAMES, LANE_ID_LABEL, LabelCardinality, LabelDescriptor, ManifestError,
    MetricDescriptor, MetricLabel, MetricManifest, MetricType, MetricUnit, NAMESPACE_LABEL,
    PARTITION_ID_LABEL, SHARD_ID_LABEL, TASK_QUEUE_LABEL, WORKER_INSTANCE_KEY_LABEL,
    all_metric_names, is_forbidden_metric_label, validate_manifest, validate_manifests,
};
pub use metrics::{
    BUILD_INFO, EMBEDDED_LIFECYCLE_OPERATIONS_TOTAL, EMBEDDED_METRIC_MANIFEST,
    PROCESS_METRIC_MANIFEST, PROCESS_START_TIME_SECONDS, install_prometheus_recorder,
    record_embedded_lifecycle, record_process_metadata,
};
pub use readiness::{
    ReadinessCheck, ReadinessCheckResult, ReadinessHandle, ReadinessRegistry, ReadinessResult,
    ReadinessStatus,
};
pub use shutdown::ObservabilityShutdown;
pub use tracing::{
    ChannelTraceContext, ErrorBiasedSamplingReason, install_tracing_subscriber,
    mark_error_biased_sample,
};

/// Errors returned by the shared observability layer.
///
/// Library callers receive typed errors so binaries can add command-specific
/// context with `anyhow` without forcing `anyhow` through shared crates.
#[derive(Debug, Error)]
pub enum ObservabilityError {
    #[error("observability configuration is invalid: {0}")]
    Config(String),
    #[error("metric manifest validation failed: {0}")]
    Manifest(String),
    #[error("metrics recorder installation failed: {0}")]
    RecorderInstall(String),
    #[error("observability HTTP server failed: {0}")]
    Http(String),
    #[error("OTLP exporter configuration failed: {0}")]
    OtlpConfig(String),
    #[error("tracing subscriber installation failed: {0}")]
    TracingInstall(String),
    #[error("readiness check failed: {0}")]
    Readiness(String),
}

static OBSERVABILITY_INSTALLED: AtomicBool = AtomicBool::new(false);

#[derive(Debug)]
struct InstallationReservation {
    committed: bool,
}

impl InstallationReservation {
    /// Reserve the one process-global observability installation slot.
    ///
    /// The reservation is intentionally separate from recorder/subscriber setup:
    /// validation runs before this point, and failed setup drops the reservation
    /// so test processes can continue with isolated cases.
    fn reserve() -> Result<Self, ObservabilityError> {
        OBSERVABILITY_INSTALLED
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| {
                ObservabilityError::RecorderInstall(
                    "observability is already installed in this process".to_string(),
                )
            })?;
        Ok(Self { committed: false })
    }

    fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for InstallationReservation {
    fn drop(&mut self) {
        if !self.committed {
            OBSERVABILITY_INSTALLED.store(false, Ordering::Release);
        }
    }
}

/// Install process-local observability and start the local telemetry endpoint.
///
/// The supplied manifests are governance inputs, not Prometheus registrations.
/// Validation catches incompatible duplicate descriptors, invalid names, and
/// unbounded labels before any global recorder or subscriber is installed. After
/// successful installation, domain crates record metrics through the normal
/// `metrics` macros and Prometheus renders whatever has been observed.
pub async fn install_observability(
    config: ProcessObservabilityConfig,
    manifests: &'static [&'static MetricManifest],
    readiness: ReadinessRegistry,
) -> Result<ObservabilityRuntime, ObservabilityError> {
    config.validate()?;
    validate_manifests(manifests)
        .map_err(|error| ObservabilityError::Manifest(error.to_string()))?;

    let reservation = InstallationReservation::reserve()?;
    let metrics_handle = install_prometheus_recorder(&config, manifests)?;
    let log_reload = install_tracing_subscriber(&config)?;
    let shutdown = ObservabilityShutdown::new();
    let http_state = ObservabilityHttpState {
        metrics: metrics_handle.clone(),
        reload: log_reload.clone(),
        readiness,
        config_snapshot: config.redacted_config.clone(),
    };
    let http_task = spawn_observability_server(config.metrics_addr, http_state, shutdown.clone());

    reservation.commit();

    Ok(ObservabilityRuntime {
        metrics_handle,
        log_reload,
        http_task,
        shutdown,
    })
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, time::Duration};

    use crate::{
        LogFormat, MetricDescriptor, MetricManifest, MetricType, MetricUnit, OtlpMetricsConfig,
        OtlpProtocol, ProcessObservabilityConfig, ServiceName, TraceExportConfig,
        install_prometheus_recorder,
    };

    use super::*;

    /// The installation flag is deliberately process-global, so the tests
    /// that reset and then assert on it must not interleave: under the
    /// default parallel harness, another reservation between one test's
    /// reset and its assert flips the outcome (observed as a rare
    /// high-core-count failure). The guard serializes exactly those tests —
    /// resetting the flag on acquisition — and leaves every other test
    /// parallel. Poisoning is ignored: a prior panicking holder cannot
    /// corrupt an `AtomicBool` we re-reset anyway.
    fn installation_test_guard() -> std::sync::MutexGuard<'static, ()> {
        static INSTALLATION_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let guard = INSTALLATION_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        OBSERVABILITY_INSTALLED.store(false, Ordering::Release);
        guard
    }

    fn test_config() -> ProcessObservabilityConfig {
        ProcessObservabilityConfig {
            service_name: ServiceName::Tokeirad,
            cluster_name: "test-cluster".to_string(),
            deployment_name: "test-deployment".to_string(),
            node_id: None,
            task_id: None,
            metrics_enabled: false,
            metrics_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            log_format: LogFormat::Text,
            log_filter: "info".to_string(),
            otlp_metrics: OtlpMetricsConfig::default(),
            tracing: TraceExportConfig::default(),
            shutdown_flush_timeout: Duration::from_secs(1),
            redacted_config: None,
        }
    }

    #[test]
    fn manifest_validation_runs_before_install_reservation() {
        let _guard = installation_test_guard();
        const INVALID_METRIC: MetricDescriptor = MetricDescriptor {
            name: "bad_counter",
            metric_type: MetricType::Counter,
            unit: MetricUnit::Count,
            description: "invalid metric name",
            labels: &[],
        };
        const INVALID_MANIFEST: MetricManifest = MetricManifest {
            crate_name: "test",
            metrics: &[INVALID_METRIC],
        };

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("observability test runtime");
        let result = runtime.block_on(install_observability(
            test_config(),
            &[&INVALID_MANIFEST],
            ReadinessRegistry::empty(),
        ));

        assert!(matches!(result, Err(ObservabilityError::Manifest(_))));
        let reservation = InstallationReservation::reserve().unwrap();
        drop(reservation);
    }

    #[test]
    fn double_install_reservation_returns_deterministic_error() {
        let _guard = installation_test_guard();
        let reservation = InstallationReservation::reserve().unwrap();

        let error = InstallationReservation::reserve().unwrap_err();

        assert!(matches!(error, ObservabilityError::RecorderInstall(_)));
        assert!(error.to_string().contains("already installed"));
        drop(reservation);
        OBSERVABILITY_INSTALLED.store(false, Ordering::Release);
    }

    #[test]
    fn disabled_metrics_mode_skips_prometheus_recorder_installation() {
        let config = test_config();

        let handle = install_prometheus_recorder(&config, &[]).unwrap();

        assert!(handle.is_none());
    }

    #[test]
    fn disabled_trace_export_does_not_require_otlp_endpoint() {
        let mut config = test_config();
        config.tracing = TraceExportConfig {
            enabled: false,
            endpoint: None,
            protocol: OtlpProtocol::Grpc,
            sample_rate: 1.0,
            error_biased_sampling: true,
        };

        let tracer = crate::tracing::install_otlp_tracer(&config).unwrap();

        assert!(tracer.is_none());
    }

    #[test]
    fn missing_required_resource_attributes_are_rejected() {
        let mut config = test_config();
        config.cluster_name.clear();

        let error = config.validate().unwrap_err();

        assert!(matches!(error, ObservabilityError::Config(_)));
        assert!(error.to_string().contains("cluster_name"));
    }
}
