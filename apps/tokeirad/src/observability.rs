use std::{net::SocketAddr, sync::Arc};

use anyhow::{Context, Result};
use metrics_exporter_prometheus::PrometheusHandle;
use serde_json::Value;
use tokeira_config::{LogFormatConfig, OtlpProtocol, TokeiraConfig};
use tokeira_edge::metrics as edge_metrics;
use tokeira_observability::{
    LogFormat, MetricManifest, ObservabilityRuntime, OtlpMetricsConfig, PROCESS_METRIC_MANIFEST,
    ProcessObservabilityConfig, ReadinessHandle, ReadinessRegistry, ReadinessStatus, ServiceName,
    TraceExportConfig, install_observability, install_prometheus_recorder,
    install_tracing_subscriber,
};
use tokeira_projection::metrics as projection_metrics;
use tokeira_runtime::metrics as runtime_metrics;
use tokeira_storage::metrics as storage_metrics;
use tokeira_types::{MetricType, validate_metric_name};
use tracing_subscriber::{EnvFilter, reload};

pub type ReloadHandle = reload::Handle<EnvFilter, tracing_subscriber::Registry>;

static PROCESS_MANIFESTS: &[&MetricManifest] = &[&PROCESS_METRIC_MANIFEST];

#[derive(Clone, Debug)]
pub(crate) struct TokeiradReadiness {
    pub registry: ReadinessRegistry,
    storage: ReadinessHandle,
    runtime: ReadinessHandle,
    routing: ReadinessHandle,
    projection: ReadinessHandle,
}

impl TokeiradReadiness {
    pub(crate) fn new() -> Self {
        let storage = readiness_handle("storage");
        let runtime = readiness_handle("runtime");
        let routing = readiness_handle("routing");
        let projection = readiness_handle("projection");
        let registry = ReadinessRegistry::from_handles(vec![
            storage.clone(),
            runtime.clone(),
            routing.clone(),
            projection.clone(),
        ]);
        Self {
            registry,
            storage,
            runtime,
            routing,
            projection,
        }
    }

    pub(crate) fn mark_started(&self) {
        self.storage.ready();
        self.runtime.ready();
        self.routing.ready();
        self.projection.ready();
    }
}

fn readiness_handle(name: &'static str) -> ReadinessHandle {
    let (_, handle) = ReadinessRegistry::mutable(
        name,
        ReadinessStatus::NotReady,
        Some("component is still starting".to_string()),
    );
    handle
}

#[derive(Clone, Debug)]
pub struct ObservabilityConfig {
    pub cluster_name: String,
    pub deployment_name: String,
    pub metrics_enabled: bool,
    pub metrics_addr: SocketAddr,
    pub otlp_enabled: bool,
    pub otlp_endpoint: String,
    pub otlp_protocol: String,
    pub trace_sample_rate: f64,
    pub log_format: LogFormat,
    pub log_filter: String,
    pub otlp_metrics: OtlpMetricsConfig,
}

impl ObservabilityConfig {
    pub fn from_tokeira_config(config: &TokeiraConfig) -> Result<Self> {
        let observability = &config.infrastructure.observability;
        let network = &config.infrastructure.network;
        let cluster_name = config.infrastructure.cluster_name.clone();
        Ok(Self {
            cluster_name: cluster_name.clone(),
            deployment_name: cluster_name,
            metrics_enabled: observability.metrics_enabled,
            metrics_addr: network.metrics_addr.parse().with_context(|| {
                format!(
                    "invalid infrastructure.network.metrics_addr value: {}",
                    network.metrics_addr
                )
            })?,
            otlp_enabled: observability.otlp_enabled,
            otlp_endpoint: observability.otlp_endpoint.clone(),
            otlp_protocol: match observability.otlp_protocol {
                OtlpProtocol::Grpc => "grpc".to_string(),
                OtlpProtocol::Http => "http".to_string(),
            },
            trace_sample_rate: observability.trace_sample_rate,
            log_format: match observability.log_format {
                LogFormatConfig::Text => LogFormat::Text,
                LogFormatConfig::Json => LogFormat::Json,
            },
            log_filter: observability.log_filter.clone(),
            otlp_metrics: OtlpMetricsConfig {
                enabled: observability.otlp_metrics.enabled,
                endpoint: observability.otlp_metrics.endpoint.clone(),
                protocol: match observability.otlp_metrics.protocol {
                    OtlpProtocol::Grpc => tokeira_observability::OtlpProtocol::Grpc,
                    OtlpProtocol::Http => tokeira_observability::OtlpProtocol::Http,
                },
                max_buffered_batches: observability.otlp_metrics.max_buffered_batches,
                export_interval: std::time::Duration::from_secs(10),
            },
        })
    }

    pub(crate) fn to_process_config(
        &self,
        redacted_config: Option<Value>,
    ) -> ProcessObservabilityConfig {
        ProcessObservabilityConfig {
            service_name: ServiceName::Tokeirad,
            cluster_name: self.cluster_name.clone(),
            deployment_name: self.deployment_name.clone(),
            node_id: None,
            task_id: None,
            metrics_enabled: self.metrics_enabled,
            metrics_addr: self.metrics_addr,
            log_format: self.log_format,
            log_filter: self.log_filter.clone(),
            otlp_metrics: self.otlp_metrics.clone(),
            tracing: TraceExportConfig {
                enabled: self.otlp_enabled,
                endpoint: Some(self.otlp_endpoint.clone()),
                protocol: if self.otlp_protocol == "http" {
                    tokeira_observability::OtlpProtocol::Http
                } else {
                    tokeira_observability::OtlpProtocol::Grpc
                },
                sample_rate: self.trace_sample_rate,
                error_biased_sampling: true,
            },
            shutdown_flush_timeout: std::time::Duration::from_secs(5),
            redacted_config,
        }
    }
}

pub async fn install_process_observability(
    config: &ObservabilityConfig,
    effective_config: Arc<TokeiraConfig>,
    readiness: ReadinessRegistry,
) -> Result<ObservabilityRuntime> {
    validate_process_metric_names().context("invalid tokeirad process metric surface")?;
    install_observability(
        config.to_process_config(Some(effective_config.to_redacted_json())),
        PROCESS_MANIFESTS,
        readiness,
    )
    .await
    .map_err(anyhow::Error::from)
    .context("failed to install tokeirad observability")
}

pub(crate) fn process_metric_names() -> Vec<(&'static str, MetricType)> {
    let mut names = Vec::new();
    names.extend_from_slice(edge_metrics::METRIC_NAMES);
    names.extend_from_slice(runtime_metrics::METRIC_NAMES);
    names.extend_from_slice(storage_metrics::METRIC_NAMES);
    names.extend_from_slice(projection_metrics::METRIC_NAMES);
    names.extend(
        PROCESS_METRIC_MANIFEST
            .metrics
            .iter()
            .map(|metric| (metric.name, metric.metric_type)),
    );
    names
}

fn validate_process_metric_names() -> Result<()> {
    for (name, kind) in process_metric_names() {
        validate_metric_name(name, kind)
            .with_context(|| format!("invalid metric name `{name}` in tokeirad process surface"))?;
    }
    Ok(())
}

pub fn install_metrics(config: &ObservabilityConfig) -> Result<Option<PrometheusHandle>> {
    install_prometheus_recorder(&config.to_process_config(None), &[])
        .map_err(anyhow::Error::from)
        .context("failed to install Prometheus metrics recorder")
}

pub fn install_tracing(config: &ObservabilityConfig) -> Result<ReloadHandle> {
    install_tracing_subscriber(&config.to_process_config(None))
        .map_err(anyhow::Error::from)
        .context("failed to install tracing subscriber")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::OnceLock;

    use http_body_util::{BodyExt, Full};
    use hyper::{Method, Request, StatusCode, body::Bytes};
    use tokeira_observability::{
        ObservabilityHttpState, ObservabilityShutdown,
        http::{handle_observability, spawn_observability_server_with_listener},
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    static METRICS_INSTALL: OnceLock<PrometheusHandle> = OnceLock::new();

    fn installed_metrics_handle() -> &'static PrometheusHandle {
        METRICS_INSTALL.get_or_init(|| {
            let config = ObservabilityConfig {
                cluster_name: "test-cluster".to_string(),
                deployment_name: "test-deployment".to_string(),
                metrics_enabled: true,
                metrics_addr: "127.0.0.1:9090".parse().unwrap(),
                otlp_enabled: false,
                otlp_endpoint: "http://localhost:4317".to_string(),
                otlp_protocol: "grpc".to_string(),
                trace_sample_rate: 1.0,
                log_format: LogFormat::Text,
                log_filter: "info".to_string(),
                otlp_metrics: OtlpMetricsConfig::default(),
            };
            install_metrics(&config)
                .unwrap()
                .expect("metrics recorder should be installed")
        })
    }

    #[test]
    fn config_from_tokeira_defaults() {
        let config = ObservabilityConfig::from_tokeira_config(&TokeiraConfig::default()).unwrap();
        assert_eq!(config.cluster_name, "tokeira-local");
        assert_eq!(config.deployment_name, "tokeira-local");
        assert!(config.metrics_enabled);
        assert_eq!(config.metrics_addr, "0.0.0.0:9090".parse().unwrap());
        assert!(!config.otlp_enabled);
        assert_eq!(config.otlp_endpoint, "http://localhost:4317");
        assert_eq!(config.otlp_protocol, "grpc");
        assert_eq!(config.trace_sample_rate, 1.0);
        assert_eq!(config.log_format, LogFormat::Text);
        assert_eq!(config.log_filter, "info");
        assert!(!config.otlp_metrics.enabled);
    }

    #[test]
    fn config_from_tokeira_respects_overrides() {
        let mut tokeira_config = TokeiraConfig::default();
        tokeira_config.infrastructure.network.metrics_addr = "127.0.0.1:9191".to_string();
        tokeira_config.infrastructure.cluster_name = "prod-a".to_string();
        tokeira_config.infrastructure.observability.metrics_enabled = false;
        tokeira_config.infrastructure.observability.otlp_enabled = true;
        tokeira_config.infrastructure.observability.otlp_endpoint = "http://tempo:4317".to_string();
        tokeira_config.infrastructure.observability.otlp_protocol = OtlpProtocol::Http;
        tokeira_config
            .infrastructure
            .observability
            .trace_sample_rate = 0.25;
        tokeira_config.infrastructure.observability.log_format = LogFormatConfig::Json;
        tokeira_config.infrastructure.observability.log_filter =
            "tokeira_runtime=debug".to_string();
        tokeira_config
            .infrastructure
            .observability
            .otlp_metrics
            .enabled = true;
        tokeira_config
            .infrastructure
            .observability
            .otlp_metrics
            .endpoint = Some("http://collector:4317".to_string());

        let config = ObservabilityConfig::from_tokeira_config(&tokeira_config).unwrap();
        assert_eq!(config.cluster_name, "prod-a");
        assert_eq!(config.deployment_name, "prod-a");
        assert!(!config.metrics_enabled);
        assert_eq!(config.metrics_addr, "127.0.0.1:9191".parse().unwrap());
        assert!(config.otlp_enabled);
        assert_eq!(config.otlp_endpoint, "http://tempo:4317");
        assert_eq!(config.otlp_protocol, "http");
        assert_eq!(config.trace_sample_rate, 0.25);
        assert_eq!(config.log_format, LogFormat::Json);
        assert_eq!(config.log_filter, "tokeira_runtime=debug");
        assert!(config.otlp_metrics.enabled);
        assert_eq!(
            config.otlp_metrics.endpoint.as_deref(),
            Some("http://collector:4317")
        );
    }

    #[test]
    fn install_metrics_returns_none_when_disabled() {
        let config = ObservabilityConfig {
            cluster_name: "test-cluster".to_string(),
            deployment_name: "test-deployment".to_string(),
            metrics_enabled: false,
            metrics_addr: "127.0.0.1:9090".parse().unwrap(),
            otlp_enabled: false,
            otlp_endpoint: "http://localhost:4317".to_string(),
            otlp_protocol: "grpc".to_string(),
            trace_sample_rate: 1.0,
            log_format: LogFormat::Text,
            log_filter: "info".to_string(),
            otlp_metrics: OtlpMetricsConfig::default(),
        };

        assert!(install_metrics(&config).unwrap().is_none());
    }

    #[test]
    fn install_metrics_exposes_build_info_metric() {
        let rendered = installed_metrics_handle().render();
        assert!(rendered.contains("# TYPE tokeira_build_info gauge"));
        assert!(rendered.contains("tokeira_build_info"));
        assert!(rendered.contains("version=\""));
        assert!(rendered.contains("commit=\""));
        assert!(rendered.contains("rustc_version=\""));
        assert!(rendered.contains("service=\"tokeirad\""));
    }

    #[test]
    fn process_config_uses_effective_cluster_identity() {
        let mut tokeira_config = TokeiraConfig::default();
        tokeira_config.infrastructure.cluster_name = "prod-eu-west-2".to_string();
        let config = ObservabilityConfig::from_tokeira_config(&tokeira_config).unwrap();

        let process = config.to_process_config(None);

        assert_eq!(process.service_name, ServiceName::Tokeirad);
        assert_eq!(process.cluster_name, "prod-eu-west-2");
        assert_eq!(process.deployment_name, "prod-eu-west-2");
        process.validate().unwrap();
    }

    #[test]
    fn process_metric_surface_includes_projection_metrics() {
        let names = process_metric_names()
            .into_iter()
            .map(|(name, _)| name)
            .collect::<Vec<_>>();

        assert!(names.contains(&projection_metrics::RECORDS_PROCESSED_TOTAL));
        assert!(names.contains(&edge_metrics::GRPC_REQUEST_TOTAL));
        assert!(names.contains(&runtime_metrics::LANE_PROCESSING_DURATION_SECONDS));
        assert!(names.contains(&storage_metrics::COMMIT_TRANSITION_DURATION_SECONDS));
    }

    #[test]
    fn process_metric_surface_validates_before_startup() {
        validate_process_metric_names().unwrap();
        tokeira_observability::validate_manifests(&[&PROCESS_METRIC_MANIFEST]).unwrap();
    }

    #[tokio::test]
    async fn tokeirad_readiness_tracks_bootstrap_state() {
        let readiness = TokeiradReadiness::new();

        let initial = readiness.registry.check_all().await;
        assert!(
            initial
                .iter()
                .all(|check| check.status == ReadinessStatus::NotReady)
        );

        readiness.mark_started();
        let started = readiness.registry.check_all().await;
        assert!(
            started
                .iter()
                .all(|check| check.status == ReadinessStatus::Ready)
        );
        assert!(started.iter().any(|check| check.name == "projection"));
    }

    #[test]
    fn log_format_defaults_to_text_and_filter_defaults_to_info() {
        let config = ObservabilityConfig::from_tokeira_config(&TokeiraConfig::default()).unwrap();
        assert_eq!(config.log_format, LogFormat::Text);
        assert_eq!(config.log_filter, "info");
    }

    #[test]
    fn reload_handle_accepts_runtime_log_level_update() {
        let (layer, handle): (reload::Layer<_, tracing_subscriber::Registry>, _) =
            reload::Layer::new(EnvFilter::new("info"));
        let _layer = layer;
        assert!(handle.reload(EnvFilter::new("debug")).is_ok());
    }

    #[tokio::test]
    async fn config_endpoint_returns_redacted_json_with_warnings() {
        let (layer, reload) = reload::Layer::new(EnvFilter::new("info"));
        let _layer = layer;
        let mut config = TokeiraConfig::default();
        let grpc_addr = config.infrastructure.network.grpc_addr.clone();
        config.infrastructure.dsql.endpoint = Some("secret.example".to_string());
        config.emergency.freeze_projection = true;
        let state = ObservabilityHttpState {
            metrics: None,
            reload,
            readiness: ReadinessRegistry::empty(),
            config_snapshot: Some(config.to_redacted_json()),
        };

        let response = handle_observability(
            Request::builder()
                .method(Method::GET)
                .uri("/config")
                .body(Full::new(Bytes::new()))
                .unwrap(),
            state,
        )
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "application/json"
        );
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["infrastructure"]["dsql"]["endpoint"], "[redacted]");
        assert_eq!(json["infrastructure"]["network"]["grpc_addr"], grpc_addr);
        assert_eq!(json["_warnings"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn metrics_endpoint_returns_prometheus_exposition() {
        let (layer, reload) = reload::Layer::new(EnvFilter::new("info"));
        let _layer = layer;
        let state = ObservabilityHttpState {
            metrics: Some(installed_metrics_handle().clone()),
            reload,
            readiness: ReadinessRegistry::empty(),
            config_snapshot: None,
        };

        let response = handle_observability(
            Request::builder()
                .method(Method::GET)
                .uri("/metrics")
                .body(Full::new(Bytes::new()))
                .unwrap(),
            state,
        )
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "text/plain; version=0.0.4"
        );
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let rendered = std::str::from_utf8(&body).unwrap();
        assert!(rendered.contains("tokeira_build_info"));
    }

    #[tokio::test]
    async fn observability_server_serves_metrics_over_http() {
        let (layer, reload) = reload::Layer::new(EnvFilter::new("info"));
        let _layer = layer;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shutdown = ObservabilityShutdown::new();
        let state = ObservabilityHttpState {
            metrics: Some(installed_metrics_handle().clone()),
            reload,
            readiness: ReadinessRegistry::empty(),
            config_snapshot: None,
        };
        let server = spawn_observability_server_with_listener(listener, state, shutdown.clone());
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();

        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        shutdown.cancel();
        server.await.unwrap().unwrap();

        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("content-type: text/plain; version=0.0.4"));
        assert!(response.contains("tokeira_build_info"));
    }

    #[test]
    fn runtime_storage_edge_metrics_appear_on_next_scrape() {
        let handle = installed_metrics_handle();
        edge_metrics::record_grpc_request("StartWorkflowExecution", "default", "ok");
        runtime_metrics::record_lane_processing_duration(
            "WorkflowTaskCompleted",
            std::time::Duration::from_millis(1),
        );
        storage_metrics::record_load_run_duration(std::time::Duration::from_millis(1));

        let rendered = handle.render();

        assert!(rendered.contains(edge_metrics::GRPC_REQUEST_TOTAL));
        assert!(rendered.contains(runtime_metrics::LANE_PROCESSING_DURATION_SECONDS));
        assert!(rendered.contains(storage_metrics::LOAD_RUN_DURATION_SECONDS));
    }
}
