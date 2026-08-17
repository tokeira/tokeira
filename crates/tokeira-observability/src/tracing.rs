//! Process tracing subscriber installation.
//!
//! This module owns global `tracing` subscriber setup and optional OTLP span
//! export. It also installs the W3C TraceContext propagator so gRPC and HTTP
//! boundaries can exchange trace context through headers.
//!
//! Runtime channel dispatch must not carry `tracing::Span` handles through
//! envelopes. Spans have subscriber-owned lifecycle state, and holding or
//! entering them across asynchronous cancellation points can couple unrelated
//! tasks. Channel correlation should instead carry raw origin trace/span IDs as
//! data attributes and create a fresh receiving span.

use opentelemetry::{
    global,
    trace::{TraceContextExt, TracerProvider as _},
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    propagation::TraceContextPropagator,
    trace::{Sampler, SdkTracer, SdkTracerProvider},
};
use serde::{Deserialize, Serialize};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, reload, util::SubscriberInitExt};

use crate::{
    LogFormat, ObservabilityError, OtlpProtocol, ProcessObservabilityConfig,
    logging::{CorrelationFormat, LogResourceFields},
};

pub type ReloadHandle = reload::Handle<EnvFilter, tracing_subscriber::Registry>;

/// Raw trace/span identifiers safe to carry through Tokio channels.
///
/// This is data, not a subscriber handle. Receivers should create their own
/// spans and record these IDs as correlation attributes instead of entering the
/// originating span across `.await` boundaries.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChannelTraceContext {
    pub origin_trace_id: [u8; 16],
    pub origin_span_id: [u8; 8],
}

/// Bounded operational failures that should bias trace capture.
///
/// The current OpenTelemetry SDK sampler makes its decision when a span starts,
/// so Tokeira cannot retroactively force-export an already head-dropped trace
/// when an error is discovered later in the async path. This marker is the
/// Phase 1 fallback from the spec: it emits a bounded event inside the current
/// span context so logs and sampled traces identify the failure class, and
/// operators can raise `sample_rate` to `1.0` during incidents.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ErrorBiasedSamplingReason {
    StorageCommitError,
    OccRetryExhausted,
    NotShardOwner,
    ProjectionSinkFailure,
    MigrationFailure,
    ControllerPlacementError,
    AutoscalerReconciliationError,
}

impl ErrorBiasedSamplingReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::StorageCommitError => "storage_commit_error",
            Self::OccRetryExhausted => "occ_retry_exhausted",
            Self::NotShardOwner => "not_shard_owner",
            Self::ProjectionSinkFailure => "projection_sink_failure",
            Self::MigrationFailure => "migration_failure",
            Self::ControllerPlacementError => "controller_placement_error",
            Self::AutoscalerReconciliationError => "autoscaler_reconciliation_error",
        }
    }
}

impl ChannelTraceContext {
    /// Capture the current OpenTelemetry span context, if one is valid.
    pub fn capture_current() -> Option<Self> {
        let context = ::tracing::Span::current().context();
        let span = context.span();
        let span_context = span.span_context();
        if span_context.is_valid() {
            Some(Self {
                origin_trace_id: span_context.trace_id().to_bytes(),
                origin_span_id: span_context.span_id().to_bytes(),
            })
        } else {
            None
        }
    }

    /// Hex-encoded trace ID for span attributes and log fields.
    pub fn origin_trace_id_hex(self) -> String {
        hex_bytes(&self.origin_trace_id)
    }

    /// Hex-encoded span ID for span attributes and log fields.
    pub fn origin_span_id_hex(self) -> String {
        hex_bytes(&self.origin_span_id)
    }
}

/// Mark the current trace/log context as operationally significant.
///
/// This intentionally records a bounded event rather than mutating sampler
/// state. Parent-based head sampling cannot be changed after a span has started,
/// but the event still gives sampled traces and structured logs a consistent
/// low-cardinality reason field.
pub fn mark_error_biased_sample(reason: ErrorBiasedSamplingReason) {
    ::tracing::warn!(
        tokeira.error_biased_sample = true,
        tokeira.error_biased_reason = reason.as_str(),
        "marked operationally significant trace failure"
    );
}

/// Install the process-global tracing subscriber.
///
/// The returned handle reloads only the `EnvFilter`; formatter shape and OTLP
/// export configuration remain fixed for the process lifetime.
pub fn install_tracing_subscriber(
    config: &ProcessObservabilityConfig,
) -> Result<ReloadHandle, ObservabilityError> {
    global::set_text_map_propagator(TraceContextPropagator::new());

    let filter =
        EnvFilter::try_new(config.log_filter.clone()).unwrap_or_else(|_| EnvFilter::new("info"));
    let (filter_layer, handle) = reload::Layer::new(filter);
    let otel_layer = install_otlp_tracer(config)?
        .map(|tracer| tracing_opentelemetry::layer().with_tracer(tracer));

    match config.log_format {
        LogFormat::Text => tracing_subscriber::registry()
            .with(filter_layer)
            .with(otel_layer)
            .with(
                tracing_subscriber::fmt::layer()
                    .event_format(CorrelationFormat::text(tracing_subscriber::fmt::format())),
            )
            .try_init()
            .map_err(|error| ObservabilityError::TracingInstall(error.to_string()))?,
        LogFormat::Json => tracing_subscriber::registry()
            .with(filter_layer)
            .with(otel_layer)
            .with(
                tracing_subscriber::fmt::layer()
                    .fmt_fields(tracing_subscriber::fmt::format::JsonFields::new())
                    .event_format(
                        CorrelationFormat::json(tracing_subscriber::fmt::format().json())
                            .with_resource(LogResourceFields::from_config(config)),
                    ),
            )
            .try_init()
            .map_err(|error| ObservabilityError::TracingInstall(error.to_string()))?,
    }

    // Subscriber registration races other threads' one-time lock-free
    // callsite registrations inside tracing-core: a callsite whose
    // registration is in flight during registration's interest rebuild can
    // be missed and stay cached `Interest::never` for the process — that
    // span or event is then silently dead until restart. Under
    // `#[tokio::main]`, runtime worker threads exist before this install
    // runs, so the window is real. Rebuilding once more after registration
    // heals every callsite whose registration has completed by now, closing
    // the startup window to a vanishing sliver. Callers must still install
    // observability before spawning application tasks.
    ::tracing::callsite::rebuild_interest_cache();

    Ok(handle)
}

/// Build and install the optional OpenTelemetry span exporter.
///
/// Metrics export is intentionally not handled here. Phase 1 uses Prometheus
/// scrape for metrics; OTLP metrics require a separate bridge or fanout design.
pub fn install_otlp_tracer(
    config: &ProcessObservabilityConfig,
) -> Result<Option<SdkTracer>, ObservabilityError> {
    if !config.tracing.enabled {
        return Ok(None);
    }

    let endpoint =
        config.tracing.endpoint.clone().ok_or_else(|| {
            ObservabilityError::OtlpConfig("trace endpoint is required".to_string())
        })?;
    let sample_rate = config.tracing.sample_rate.clamp(0.0, 1.0);
    let exporter = match config.tracing.protocol {
        OtlpProtocol::Http => opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_endpoint(endpoint)
            .build()
            .map_err(|error| ObservabilityError::OtlpConfig(error.to_string()))?,
        OtlpProtocol::Grpc => opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build()
            .map_err(|error| ObservabilityError::OtlpConfig(error.to_string()))?,
    };

    let provider = SdkTracerProvider::builder()
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
            sample_rate,
        ))))
        .with_batch_exporter(exporter)
        .build();
    let tracer = provider.tracer(config.service_name.as_str());
    global::set_tracer_provider(provider);
    Ok(Some(tracer))
}

fn hex_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use opentelemetry::trace::TracerProvider;
    use tracing_subscriber::layer::SubscriberExt;

    use super::*;

    #[test]
    fn channel_trace_context_formats_origin_ids() {
        let context = ChannelTraceContext {
            origin_trace_id: [1; 16],
            origin_span_id: [2; 8],
        };

        assert_eq!(
            context.origin_trace_id_hex(),
            "01010101010101010101010101010101"
        );
        assert_eq!(context.origin_span_id_hex(), "0202020202020202");
    }

    #[test]
    fn captures_current_otel_span_context() {
        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder().build();
        let tracer = provider.tracer("test");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
        let dispatch = tracing::Dispatch::new(subscriber);

        let captured = tracing::dispatcher::with_default(&dispatch, || {
            let span = tracing::info_span!("dispatch");
            let _entered = span.enter();
            ChannelTraceContext::capture_current()
        });

        assert!(captured.is_some());
    }

    #[test]
    fn error_biased_sampling_reasons_are_bounded() {
        let reasons = [
            ErrorBiasedSamplingReason::StorageCommitError,
            ErrorBiasedSamplingReason::OccRetryExhausted,
            ErrorBiasedSamplingReason::NotShardOwner,
            ErrorBiasedSamplingReason::ProjectionSinkFailure,
            ErrorBiasedSamplingReason::MigrationFailure,
            ErrorBiasedSamplingReason::ControllerPlacementError,
            ErrorBiasedSamplingReason::AutoscalerReconciliationError,
        ];

        for reason in reasons {
            assert!(!reason.as_str().is_empty());
            assert!(
                reason
                    .as_str()
                    .chars()
                    .all(|ch| { ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_' })
            );
        }
    }
}
