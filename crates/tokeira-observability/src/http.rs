//! Local observability HTTP endpoint.
//!
//! This server is process-local operational plumbing, not the public Temporal
//! API. It exposes Prometheus metrics, liveness, readiness, a redacted config
//! snapshot, and a runtime log-level update endpoint. Callers are expected to
//! bind it only where the deployment's operational network model permits.

use std::{convert::Infallible, fmt::Display, net::SocketAddr};

use http_body_util::{BodyExt, Full};
use hyper::{
    Method, Request, Response, StatusCode, body::Bytes, server::conn::http1, service::service_fn,
};
use hyper_util::rt::TokioIo;
use metrics_exporter_prometheus::PrometheusHandle;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tracing_subscriber::{EnvFilter, reload};

use crate::{ObservabilityError, ObservabilityShutdown, ReadinessRegistry};

/// Redacted configuration document exposed on `/config`.
pub type ConfigSnapshot = Value;

/// Immutable state shared by observability HTTP connections.
#[derive(Clone)]
pub struct ObservabilityHttpState {
    /// Prometheus render handle; absent when metrics are disabled.
    pub metrics: Option<PrometheusHandle>,
    /// Reload handle for the process log filter.
    pub reload: reload::Handle<EnvFilter, tracing_subscriber::Registry>,
    /// Readiness checks backing `/readyz`.
    pub readiness: ReadinessRegistry,
    /// Already-redacted config snapshot. This server never receives raw config
    /// because redaction belongs at the config boundary.
    pub config_snapshot: Option<ConfigSnapshot>,
}

impl std::fmt::Debug for ObservabilityHttpState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ObservabilityHttpState")
            .field("metrics", &self.metrics.is_some())
            .field("readiness", &self.readiness)
            .field("config_snapshot", &self.config_snapshot.is_some())
            .finish_non_exhaustive()
    }
}

/// Spawn the HTTP server and stop it when the shutdown token is cancelled.
pub fn spawn_observability_server(
    addr: SocketAddr,
    state: ObservabilityHttpState,
    shutdown: ObservabilityShutdown,
) -> tokio::task::JoinHandle<Result<(), ObservabilityError>> {
    tokio::spawn(async move {
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|error| ObservabilityError::Http(error.to_string()))?;
        serve_observability_listener(listener, state, shutdown).await
    })
}

/// Spawn the HTTP server from an already-bound listener.
///
/// Production callers normally use `spawn_observability_server`. Tests use this
/// helper to bind `127.0.0.1:0`, discover the selected port, and exercise the
/// real socket path without fixed ports or sleeps.
pub fn spawn_observability_server_with_listener(
    listener: TcpListener,
    state: ObservabilityHttpState,
    shutdown: ObservabilityShutdown,
) -> tokio::task::JoinHandle<Result<(), ObservabilityError>> {
    tokio::spawn(serve_observability_listener(listener, state, shutdown))
}

async fn serve_observability_listener(
    listener: TcpListener,
    state: ObservabilityHttpState,
    shutdown: ObservabilityShutdown,
) -> Result<(), ObservabilityError> {
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            accepted = listener.accept() => {
                let (stream, _) = accepted.map_err(|error| ObservabilityError::Http(error.to_string()))?;
                let state = state.clone();
                tokio::spawn(async move {
                    let io = TokioIo::new(stream);
                    let service = service_fn(move |request| {
                        let state = state.clone();
                        async move { handle_observability(request, state).await }
                    });
                    if let Err(error) = http1::Builder::new().serve_connection(io, service).await {
                        tracing::warn!(?error, "observability connection failed");
                    }
                });
            }
        }
    }
}

/// Handle one observability HTTP request.
///
/// The generic body type keeps this handler testable without binding a socket.
pub async fn handle_observability<B>(
    request: Request<B>,
    state: ObservabilityHttpState,
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
                .map(PrometheusHandle::render)
                .unwrap_or_default();
            response(StatusCode::OK, body, Some("text/plain; version=0.0.4"))
        }
        (&Method::GET, "/healthz") => response(
            StatusCode::OK,
            json!({"status":"ok"}).to_string(),
            Some("application/json"),
        ),
        (&Method::GET, "/readyz") => {
            let checks = state.readiness.check_all().await;
            let ready = checks
                .iter()
                .all(|check| check.status == crate::ReadinessStatus::Ready);
            let status = if ready {
                StatusCode::OK
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            };
            response(
                status,
                json!({"status": if ready { "ready" } else { "not_ready" }, "checks": checks})
                    .to_string(),
                Some("application/json"),
            )
        }
        (&Method::GET, "/config") => {
            let body = state
                .config_snapshot
                .as_ref()
                .cloned()
                .unwrap_or_else(|| json!({"status":"unavailable"}))
                .to_string();
            response(StatusCode::OK, body, Some("application/json"))
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

fn response(status: StatusCode, body: String, content_type: Option<&str>) -> Response<Full<Bytes>> {
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
    use std::{sync::Arc, time::Duration};

    use async_trait::async_trait;
    use http_body_util::Full;
    use hyper::body::Bytes;
    use tracing_subscriber::{EnvFilter, reload};

    use super::*;
    use crate::{ReadinessCheck, ReadinessCheckResult, ReadinessStatus};

    #[derive(Debug)]
    struct StaticCheck {
        name: &'static str,
        status: ReadinessStatus,
    }

    #[async_trait]
    impl ReadinessCheck for StaticCheck {
        fn name(&self) -> &'static str {
            self.name
        }

        async fn check(&self) -> Result<ReadinessCheckResult, crate::ObservabilityError> {
            Ok(ReadinessCheckResult::new(self.status))
        }
    }

    fn test_state(readiness: ReadinessRegistry) -> ObservabilityHttpState {
        let (layer, reload): (reload::Layer<_, tracing_subscriber::Registry>, _) =
            reload::Layer::new(EnvFilter::new("info"));
        let _layer = layer;
        ObservabilityHttpState {
            metrics: None,
            reload,
            readiness,
            config_snapshot: None,
        }
    }

    async fn request(path: &str, state: ObservabilityHttpState) -> Response<Full<Bytes>> {
        handle_observability(
            Request::builder()
                .method(Method::GET)
                .uri(path)
                .body(Full::new(Bytes::new()))
                .unwrap(),
            state,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn healthz_returns_ok_even_when_readiness_fails() {
        let readiness = ReadinessRegistry::new(vec![Arc::new(StaticCheck {
            name: "storage",
            status: ReadinessStatus::NotReady,
        })]);

        let response = request("/healthz", test_state(readiness)).await;

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn readyz_reports_failing_check() {
        let readiness = ReadinessRegistry::new(vec![Arc::new(StaticCheck {
            name: "storage",
            status: ReadinessStatus::NotReady,
        })]);

        let response = request("/readyz", test_state(readiness)).await;

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "not_ready");
        assert_eq!(json["checks"][0]["name"], "storage");
        assert_eq!(json["checks"][0]["status"], "not_ready");
        assert!(json["checks"][0]["message"].is_null());
        assert!(json["checks"][0]["checked_at_unix_seconds"].is_u64());
        assert!(json["checks"][0]["latency_millis"].is_u64());
    }

    #[tokio::test]
    async fn metrics_returns_empty_body_when_disabled() {
        let response = request("/metrics", test_state(ReadinessRegistry::empty())).await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "text/plain; version=0.0.4"
        );
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn observability_server_stops_when_shutdown_is_cancelled() {
        let shutdown = ObservabilityShutdown::new();
        let handle = spawn_observability_server(
            "127.0.0.1:0".parse().unwrap(),
            test_state(ReadinessRegistry::empty()),
            shutdown.clone(),
        );

        shutdown.cancel();

        let result = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("observability server did not stop after cancellation")
            .expect("observability server task panicked");
        assert!(result.is_ok());
    }
}
