use std::{convert::Infallible, fmt::Display, net::SocketAddr, sync::Arc};

use anyhow::{Context, Result};
use http_body_util::{BodyExt, Full};
use hyper::{
    Method, Request, Response, StatusCode, body::Bytes, server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::TokioIo;
use metrics::gauge;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use opentelemetry::{global, trace::TracerProvider as _};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    propagation::TraceContextPropagator,
    trace::{Sampler, SdkTracer, SdkTracerProvider},
};
use tokio::net::TcpListener;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use crate::correlation_format::CorrelationFormat;
use tokeira_config::{LogFormatConfig, OtlpProtocol, TokeiraConfig};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogFormat {
    Text,
    Json,
}

#[derive(Clone, Debug)]
pub struct ObservabilityConfig {
    pub metrics_enabled: bool,
    pub metrics_addr: SocketAddr,
    pub otlp_enabled: bool,
    pub otlp_endpoint: String,
    pub otlp_protocol: String,
    pub trace_sample_rate: f64,
    pub log_format: LogFormat,
    pub log_filter: String,
}

pub type ReloadHandle =
    tracing_subscriber::reload::Handle<EnvFilter, tracing_subscriber::Registry>;

#[derive(Clone)]
struct ObservabilityServerState {
    metrics: Option<PrometheusHandle>,
    reload: ReloadHandle,
    config: Arc<TokeiraConfig>,
}

impl ObservabilityConfig {
    pub fn from_tokeira_config(config: &TokeiraConfig) -> Result<Self> {
        let observability = &config.infrastructure.observability;
        let network = &config.infrastructure.network;
        Ok(Self {
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
        })
    }
}

pub fn install_metrics(config: &ObservabilityConfig) -> Result<Option<PrometheusHandle>> {
    if !config.metrics_enabled {
        return Ok(None);
    }

    let handle = PrometheusBuilder::new()
        .install_recorder()
        .context("failed to install Prometheus metrics recorder")?;

    gauge!(
        "tokeira_build_info",
        "version" => env!("CARGO_PKG_VERSION").to_string(),
        "commit" => option_env!("TOKEIRA_GIT_COMMIT").unwrap_or("unknown").to_string(),
        "rustc_version" => option_env!("RUSTC_VERSION").unwrap_or("unknown").to_string(),
    )
    .set(1.0);

    Ok(Some(handle))
}

pub fn install_tracing(config: &ObservabilityConfig) -> Result<ReloadHandle> {
    global::set_text_map_propagator(TraceContextPropagator::new());

    let filter = EnvFilter::try_new(config.log_filter.clone())
        .unwrap_or_else(|_| EnvFilter::new("info"));
    let (filter_layer, handle) = tracing_subscriber::reload::Layer::new(filter);
    let otel_layer = install_otlp_tracer(config)?
        .map(|tracer| tracing_opentelemetry::layer().with_tracer(tracer));

    match config.log_format {
        LogFormat::Text => {
            tracing_subscriber::registry()
                .with(filter_layer)
                .with(otel_layer)
                .with(tracing_subscriber::fmt::layer().event_format(
                    CorrelationFormat::text(tracing_subscriber::fmt::format()),
                ))
                .try_init()
                .context("failed to install text tracing subscriber")?
        }
        LogFormat::Json => tracing_subscriber::registry()
            .with(filter_layer)
            .with(otel_layer)
            .with(
                tracing_subscriber::fmt::layer()
                    .fmt_fields(tracing_subscriber::fmt::format::JsonFields::new())
                    .event_format(CorrelationFormat::json(
                        tracing_subscriber::fmt::format().json(),
                    )),
            )
            .try_init()
            .context("failed to install JSON tracing subscriber")?,
    }

    Ok(handle)
}

fn install_otlp_tracer(config: &ObservabilityConfig) -> Result<Option<SdkTracer>> {
    if !config.otlp_enabled {
        return Ok(None);
    }

    let sample_rate = config.trace_sample_rate.clamp(0.0, 1.0);
    let exporter = match config.otlp_protocol.as_str() {
        "http" => opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_endpoint(config.otlp_endpoint.clone())
            .build()
            .context("failed to build OTLP HTTP span exporter")?,
        _ => opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(config.otlp_endpoint.clone())
            .build()
            .context("failed to build OTLP gRPC span exporter")?,
    };

    let provider = SdkTracerProvider::builder()
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
            sample_rate,
        ))))
        .with_batch_exporter(exporter)
        .build();
    let tracer = provider.tracer("tokeirad");
    global::set_tracer_provider(provider);
    Ok(Some(tracer))
}

pub fn spawn_observability_server(
    config: &ObservabilityConfig,
    effective_config: Arc<TokeiraConfig>,
    metrics: Option<PrometheusHandle>,
    reload: ReloadHandle,
) -> tokio::task::JoinHandle<Result<()>> {
    let addr = config.metrics_addr;
    let state = ObservabilityServerState {
        metrics,
        reload,
        config: effective_config,
    };
    tokio::spawn(async move {
        let listener = TcpListener::bind(addr).await.with_context(|| {
            format!("failed to bind observability listener on {addr}")
        })?;
        loop {
            let (stream, _) = listener.accept().await?;
            let state = state.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let service = service_fn(move |request| {
                    let state = state.clone();
                    async move { handle_observability(request, state).await }
                });
                if let Err(error) =
                    http1::Builder::new().serve_connection(io, service).await
                {
                    tracing::warn!(?error, "observability connection failed");
                }
            });
        }
    })
}

async fn handle_observability<B>(
    request: Request<B>,
    state: ObservabilityServerState,
) -> Result<Response<Full<Bytes>>, Infallible>
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
    B::Error: Display,
{
    let response = match (request.method(), request.uri().path()) {
        (&Method::GET, "/metrics") => {
            let body = state
                .metrics
                .as_ref()
                .map(|handle| handle.render())
                .unwrap_or_default();
            response(StatusCode::OK, body, Some("text/plain; version=0.0.4"))
        }
        (&Method::GET, "/config") => {
            match serde_json::to_string(&state.config.to_redacted_json()) {
                Ok(body) => response(StatusCode::OK, body, Some("application/json")),
                Err(error) => response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed to serialize config: {error}"),
                    None,
                ),
            }
        }
        (&Method::PUT, "/loglevel") => {
            let body = match request.into_body().collect().await {
                Ok(collected) => collected.to_bytes(),
                Err(error) => {
                    return Ok(response(
                        StatusCode::BAD_REQUEST,
                        format!("failed to read request body: {error}"),
                        None,
                    ));
                }
            };
            match std::str::from_utf8(&body)
                .ok()
                .and_then(|value| EnvFilter::try_new(value.trim()).ok())
            {
                Some(filter) => match state.reload.reload(filter) {
                    Ok(()) => response(StatusCode::OK, "ok".to_string(), None),
                    Err(error) => response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("failed to apply log filter: {error}"),
                        None,
                    ),
                },
                None => response(
                    StatusCode::BAD_REQUEST,
                    "invalid RUST_LOG filter".to_string(),
                    None,
                ),
            }
        }
        _ => response(StatusCode::NOT_FOUND, "not found".to_string(), None),
    };
    Ok(response)
}

fn response(
    status: StatusCode,
    body: String,
    content_type: Option<&str>,
) -> Response<Full<Bytes>> {
    let mut builder = Response::builder().status(status);
    if let Some(content_type) = content_type {
        builder = builder.header("content-type", content_type);
    }
    builder
        .body(Full::new(Bytes::from(body)))
        .expect("response builder should be infallible")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::OnceLock;

    static METRICS_INSTALL: OnceLock<PrometheusHandle> = OnceLock::new();

    fn installed_metrics_handle() -> &'static PrometheusHandle {
        METRICS_INSTALL.get_or_init(|| {
            let config = ObservabilityConfig {
                metrics_enabled: true,
                metrics_addr: "127.0.0.1:9090".parse().unwrap(),
                otlp_enabled: false,
                otlp_endpoint: "http://localhost:4317".to_string(),
                otlp_protocol: "grpc".to_string(),
                trace_sample_rate: 1.0,
                log_format: LogFormat::Text,
                log_filter: "info".to_string(),
            };
            install_metrics(&config)
                .unwrap()
                .expect("metrics recorder should be installed")
        })
    }

    #[test]
    fn config_from_tokeira_defaults() {
        let config =
            ObservabilityConfig::from_tokeira_config(&TokeiraConfig::default()).unwrap();
        assert!(config.metrics_enabled);
        assert_eq!(config.metrics_addr, "0.0.0.0:9090".parse().unwrap());
        assert!(!config.otlp_enabled);
        assert_eq!(config.otlp_endpoint, "http://localhost:4317");
        assert_eq!(config.otlp_protocol, "grpc");
        assert_eq!(config.trace_sample_rate, 1.0);
        assert_eq!(config.log_format, LogFormat::Text);
        assert_eq!(config.log_filter, "info");
    }

    #[test]
    fn config_from_tokeira_respects_overrides() {
        let mut tokeira_config = TokeiraConfig::default();
        tokeira_config.infrastructure.network.metrics_addr = "127.0.0.1:9191".to_string();
        tokeira_config.infrastructure.observability.metrics_enabled = false;
        tokeira_config.infrastructure.observability.otlp_enabled = true;
        tokeira_config.infrastructure.observability.otlp_endpoint =
            "http://tempo:4317".to_string();
        tokeira_config.infrastructure.observability.otlp_protocol = OtlpProtocol::Http;
        tokeira_config
            .infrastructure
            .observability
            .trace_sample_rate = 0.25;
        tokeira_config.infrastructure.observability.log_format = LogFormatConfig::Json;
        tokeira_config.infrastructure.observability.log_filter =
            "tokeira_runtime=debug".to_string();

        let config = ObservabilityConfig::from_tokeira_config(&tokeira_config).unwrap();
        assert!(!config.metrics_enabled);
        assert_eq!(config.metrics_addr, "127.0.0.1:9191".parse().unwrap());
        assert!(config.otlp_enabled);
        assert_eq!(config.otlp_endpoint, "http://tempo:4317");
        assert_eq!(config.otlp_protocol, "http");
        assert_eq!(config.trace_sample_rate, 0.25);
        assert_eq!(config.log_format, LogFormat::Json);
        assert_eq!(config.log_filter, "tokeira_runtime=debug");
    }

    #[test]
    fn install_metrics_returns_none_when_disabled() {
        let config = ObservabilityConfig {
            metrics_enabled: false,
            metrics_addr: "127.0.0.1:9090".parse().unwrap(),
            otlp_enabled: false,
            otlp_endpoint: "http://localhost:4317".to_string(),
            otlp_protocol: "grpc".to_string(),
            trace_sample_rate: 1.0,
            log_format: LogFormat::Text,
            log_filter: "info".to_string(),
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
    }

    #[test]
    fn install_otlp_tracer_returns_none_when_disabled() {
        let config = ObservabilityConfig {
            metrics_enabled: false,
            metrics_addr: "127.0.0.1:9090".parse().unwrap(),
            otlp_enabled: false,
            otlp_endpoint: "http://localhost:4317".to_string(),
            otlp_protocol: "grpc".to_string(),
            trace_sample_rate: 1.0,
            log_format: LogFormat::Text,
            log_filter: "info".to_string(),
        };

        assert!(install_otlp_tracer(&config).unwrap().is_none());
    }

    #[test]
    fn log_format_defaults_to_text_and_filter_defaults_to_info() {
        let config =
            ObservabilityConfig::from_tokeira_config(&TokeiraConfig::default()).unwrap();
        assert_eq!(config.log_format, LogFormat::Text);
        assert_eq!(config.log_filter, "info");
    }

    #[test]
    fn reload_handle_accepts_runtime_log_level_update() {
        let (layer, handle): (
            tracing_subscriber::reload::Layer<_, tracing_subscriber::Registry>,
            _,
        ) = tracing_subscriber::reload::Layer::new(EnvFilter::new("info"));
        let _layer = layer;
        assert!(handle.reload(EnvFilter::new("debug")).is_ok());
    }

    #[tokio::test]
    async fn config_endpoint_returns_redacted_json_with_warnings() {
        let (layer, reload) =
            tracing_subscriber::reload::Layer::new(EnvFilter::new("info"));
        let _layer = layer;
        let mut config = TokeiraConfig::default();
        config.infrastructure.dsql.endpoint = Some("secret.example".to_string());
        config.emergency.freeze_projection = true;
        let state = ObservabilityServerState {
            metrics: None,
            reload,
            config: Arc::new(config),
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
        assert_eq!(json["infrastructure"]["network"]["grpc_addr"], "[::1]:7233");
        assert_eq!(json["_warnings"].as_array().unwrap().len(), 1);
    }
}
