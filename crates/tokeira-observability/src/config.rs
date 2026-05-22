//! Configuration types shared by the process observability bootstrap.
//!
//! These types are intentionally small and serializable where they cross config
//! boundaries. They describe how a binary wants observability installed; they do
//! not own any domain metric definitions or process lifecycle policy.

use std::{fmt, net::SocketAddr, time::Duration};

use metrics_exporter_prometheus::PrometheusHandle;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tracing_subscriber::{EnvFilter, reload};

use crate::{ConfigSnapshot, ObservabilityError, ObservabilityShutdown};

/// Process-local observability configuration for one Tokeira binary.
///
/// Binaries assemble this from their own runtime config after redaction and
/// environment-specific defaults have been applied. The shared installer treats
/// the values as final and does not reach back into deployment config files.
#[derive(Clone, Debug)]
pub struct ProcessObservabilityConfig {
    /// Stable process identity used by process metadata metrics and log fields.
    pub service_name: ServiceName,
    /// Operator-visible cluster name; bounded as configuration, not per request.
    pub cluster_name: String,
    /// Operator-visible deployment name; used to distinguish blue/green or
    /// branch deployments sharing the same observability backend.
    pub deployment_name: String,
    /// Optional runtime/controller node identity when the process has one.
    pub node_id: Option<String>,
    /// Optional platform task identity, such as an ECS task ID.
    pub task_id: Option<String>,
    /// Enables the process-global Prometheus recorder and `/metrics` output.
    pub metrics_enabled: bool,
    /// Bind address for the local observability HTTP server.
    pub metrics_addr: SocketAddr,
    /// Log format selected by the owning binary. Production mode generally uses
    /// JSON; local developer commands may keep text output.
    pub log_format: LogFormat,
    /// Runtime-reloadable tracing filter expression.
    pub log_filter: String,
    /// Deferred Phase 2 metrics-push configuration.
    pub otlp_metrics: OtlpMetricsConfig,
    /// OpenTelemetry trace export configuration.
    pub tracing: TraceExportConfig,
    /// Best-effort flush budget used by process shutdown paths.
    pub shutdown_flush_timeout: Duration,
    /// Already-redacted config snapshot exposed on `/config`.
    pub redacted_config: Option<ConfigSnapshot>,
}

/// Stable service labels used by process metadata and scrape labels.
///
/// Projection does not appear as a Phase 1 service because projection workers
/// currently run embedded in `tokeirad`; their metrics are distinguished by the
/// `tokeira_projection_*` prefix on the same scrape target.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ServiceName {
    Tokeirad,
    Controller,
    Autoscaler,
}

impl ServiceName {
    /// Return the canonical metric/log label value.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Tokeirad => "tokeirad",
            Self::Controller => "tokeira-controller",
            Self::Autoscaler => "tokeira-autoscaler",
        }
    }
}

impl fmt::Display for ServiceName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl ProcessObservabilityConfig {
    /// Validate only invariants owned by the shared observability layer.
    ///
    /// Domain-specific checks belong in the binary or platform config parser so
    /// this crate can remain dependency-neutral.
    pub fn validate(&self) -> Result<(), ObservabilityError> {
        if self.cluster_name.trim().is_empty() {
            return Err(ObservabilityError::Config(
                "cluster_name must not be empty".to_string(),
            ));
        }
        if self.deployment_name.trim().is_empty() {
            return Err(ObservabilityError::Config(
                "deployment_name must not be empty".to_string(),
            ));
        }
        if !(0.0..=1.0).contains(&self.tracing.sample_rate) || !self.tracing.sample_rate.is_finite()
        {
            return Err(ObservabilityError::Config(
                "trace sample_rate must be finite and between 0.0 and 1.0".to_string(),
            ));
        }
        Ok(())
    }
}

/// Process log output format.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum LogFormat {
    /// Human-readable local output.
    Text,
    /// Structured production output with process and trace correlation fields.
    Json,
}

/// Deferred Phase 2 OTLP metrics configuration.
///
/// Tokeira currently installs a single `metrics` recorder for Prometheus scrape.
/// OTLP metric push requires either a fanout recorder, an Alloy bridge, or a
/// scrape-and-push bridge; the config is modeled now so deployment config can be
/// stable while implementation remains explicitly deferred.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct OtlpMetricsConfig {
    /// User intent for Phase 2 push export.
    pub enabled: bool,
    /// Collector endpoint once Phase 2 export is implemented.
    pub endpoint: Option<String>,
    /// Transport selected for the future exporter or bridge.
    pub protocol: OtlpProtocol,
    /// Backpressure budget for the future push path.
    pub max_buffered_batches: usize,
    /// Export cadence for the future push path; skipped during config serde
    /// because `std::time::Duration` is not part of the stable TOML contract.
    #[serde(skip)]
    pub export_interval: Duration,
}

/// OTLP trace export configuration.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TraceExportConfig {
    /// Enables OpenTelemetry span export.
    pub enabled: bool,
    /// Collector endpoint; required when trace export is enabled.
    pub endpoint: Option<String>,
    /// OTLP transport protocol used for spans.
    pub protocol: OtlpProtocol,
    /// Head sampling ratio applied through a parent-based sampler.
    pub sample_rate: f64,
    /// Enables bounded error markers for operationally significant failures.
    ///
    /// This does not override the OpenTelemetry head sampler after a span has
    /// started; it records a consistent marker in sampled traces and logs.
    #[serde(default = "default_error_biased_sampling")]
    pub error_biased_sampling: bool,
}

/// OTLP transport protocol.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum OtlpProtocol {
    Grpc,
    Http,
}

/// Handles returned after observability installation.
///
/// The returned value is intentionally owned by the binary. Dropping it does not
/// uninstall global recorders or subscribers, but the shutdown token and HTTP
/// task handle let the binary stop background work cooperatively.
pub struct ObservabilityRuntime {
    /// Prometheus render handle; absent when metrics are disabled.
    pub metrics_handle: Option<PrometheusHandle>,
    /// Runtime log filter reload handle used by `/loglevel`.
    pub log_reload: reload::Handle<EnvFilter, tracing_subscriber::Registry>,
    /// Local observability HTTP server task.
    pub http_task: JoinHandle<Result<(), ObservabilityError>>,
    /// Shared cancellation token for observability-owned background work.
    pub shutdown: ObservabilityShutdown,
}

impl fmt::Debug for ObservabilityRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ObservabilityRuntime")
            .field("metrics_handle", &self.metrics_handle.is_some())
            .field("http_task", &self.http_task)
            .field("shutdown", &self.shutdown)
            .finish_non_exhaustive()
    }
}

impl Default for OtlpMetricsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: None,
            protocol: OtlpProtocol::Grpc,
            max_buffered_batches: 1024,
            export_interval: Duration::from_secs(10),
        }
    }
}

impl Default for TraceExportConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: None,
            protocol: OtlpProtocol::Grpc,
            sample_rate: 1.0,
            error_biased_sampling: true,
        }
    }
}

fn default_error_biased_sampling() -> bool {
    true
}
