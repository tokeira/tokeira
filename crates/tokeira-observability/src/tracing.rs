//! Process tracing subscriber installation.
//!
//! This module owns global `tracing` subscriber setup and optional OTLP span
//! export. It also installs the W3C TraceContext propagator so gRPC and HTTP
//! boundaries can exchange trace context through headers.
//!
//! Runtime channel dispatch must not carry `tracing::Span` handles through
//! envelopes. Spans have subscriber-owned lifecycle state, and holding or
//! entering them across asynchronous cancellation points can couple unrelated
//! tasks. Channel correlation instead carries the immutable W3C span context as
//! data and creates a fresh receiving span with an explicit remote parent.

use opentelemetry::{
    Context, global,
    trace::{
        SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState, TracerProvider as _,
    },
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

/// Serializable W3C span context safe to carry through process-local channels.
///
/// This is data, not a subscriber handle. Receivers rebuild a remote parent and
/// create their own span instead of entering the originating span across
/// `.await` boundaries. It is transient execution context and must never be
/// added to workflow history or another authoritative record.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChannelTraceContext {
    /// W3C trace identifier.
    pub trace_id: [u8; 16],
    /// W3C parent span identifier.
    pub span_id: [u8; 8],
    /// W3C trace flags, including the sampled bit.
    pub trace_flags: u8,
    /// Ordered W3C vendor trace state.
    pub trace_state: String,
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
                trace_id: span_context.trace_id().to_bytes(),
                span_id: span_context.span_id().to_bytes(),
                trace_flags: span_context.trace_flags().to_u8(),
                trace_state: span_context.trace_state().header(),
            })
        } else {
            None
        }
    }

    /// Rebuild this data as a remote OpenTelemetry parent.
    ///
    /// Invalid IDs or tracestate produce an empty context. Channel data may be
    /// deserialized from an untrusted or older process, and invalid telemetry
    /// must start a root rather than affect execution.
    pub fn as_remote_parent(&self) -> Context {
        let Ok(trace_state) = self.trace_state.parse::<TraceState>() else {
            return Context::new();
        };
        let span_context = SpanContext::new(
            TraceId::from_bytes(self.trace_id),
            SpanId::from_bytes(self.span_id),
            TraceFlags::new(self.trace_flags),
            true,
            trace_state,
        );
        if span_context.is_valid() {
            Context::new().with_remote_span_context(span_context)
        } else {
            Context::new()
        }
    }

    /// Hex-encoded trace ID for stable correlation attributes and log fields.
    pub fn trace_id_hex(&self) -> String {
        hex_bytes(&self.trace_id)
    }

    /// Hex-encoded span ID for stable correlation attributes and log fields.
    pub fn span_id_hex(&self) -> String {
        hex_bytes(&self.span_id)
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
    use std::collections::HashMap;

    use opentelemetry::{
        propagation::{Extractor, Injector, TextMapPropagator},
        trace::TracerProvider,
    };
    use opentelemetry_sdk::propagation::TraceContextPropagator;
    use proptest::prelude::*;
    use tracing_subscriber::layer::SubscriberExt;

    use super::*;

    #[test]
    fn channel_trace_context_formats_origin_ids() {
        let context = ChannelTraceContext {
            trace_id: [1; 16],
            span_id: [2; 8],
            trace_flags: TraceFlags::SAMPLED.to_u8(),
            trace_state: "vendor=value".to_owned(),
        };

        assert_eq!(context.trace_id_hex(), "01010101010101010101010101010101");
        assert_eq!(context.span_id_hex(), "0202020202020202");
        let parent = context.as_remote_parent();
        let parent_span = parent.span();
        assert!(parent_span.span_context().is_remote());
        assert_eq!(
            parent_span.span_context().trace_flags(),
            TraceFlags::SAMPLED
        );
        assert_eq!(
            parent_span.span_context().trace_state().header(),
            "vendor=value"
        );
    }

    #[test]
    fn invalid_channel_context_rebuilds_as_root() {
        let context = ChannelTraceContext {
            trace_id: [0; 16],
            span_id: [0; 8],
            trace_flags: 0,
            trace_state: "not-valid-tracestate".to_owned(),
        };

        assert!(!context.as_remote_parent().span().span_context().is_valid());
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

    #[derive(Clone, Copy, Debug)]
    enum Boundary {
        Service,
        DirectChannel,
        WorkflowTask,
        ActivityTask,
        Outbound,
        Handoff,
        Restart,
    }

    fn boundary_strategy() -> impl Strategy<Value = Boundary> {
        prop_oneof![
            Just(Boundary::Service),
            Just(Boundary::DirectChannel),
            Just(Boundary::WorkflowTask),
            Just(Boundary::ActivityTask),
            Just(Boundary::Outbound),
            Just(Boundary::Handoff),
            Just(Boundary::Restart),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        // Feature: managed-embedded-dsql, Property 16: transient context and durable identifiers compose
        #[test]
        fn transient_context_and_durable_identifiers_compose(
            trace_id in any::<[u8; 16]>(),
            span_id in any::<[u8; 8]>(),
            sampled in any::<bool>(),
            boundaries in prop::collection::vec(boundary_strategy(), 1..32),
            workflow_id in "[a-zA-Z0-9_-]{1,32}",
            run_id in "[a-zA-Z0-9-]{1,36}",
            activity_id in "[a-zA-Z0-9_-]{1,32}",
            attempt in 1u32..1000,
        ) {
            prop_assume!(trace_id.iter().any(|byte| *byte != 0));
            prop_assume!(span_id.iter().any(|byte| *byte != 0));
            let initial = ChannelTraceContext {
                trace_id,
                span_id,
                trace_flags: if sampled { TraceFlags::SAMPLED.to_u8() } else { 0 },
                trace_state: "host=tokeira".to_owned(),
            };
            let mut live_context = Some(initial.clone());
            let mut observed_relationships = Vec::new();
            let durable_ids = serde_json::json!({
                "workflow_id": workflow_id.clone(),
                "run_id": run_id.clone(),
                "activity_id": activity_id.clone(),
                "attempt": attempt,
            });

            for boundary in boundaries {
                match boundary {
                    Boundary::Restart => live_context = None,
                    Boundary::Handoff => {
                        observed_relationships.push(("link", live_context.clone()));
                    }
                    Boundary::Service
                    | Boundary::DirectChannel
                    | Boundary::WorkflowTask
                    | Boundary::ActivityTask
                    | Boundary::Outbound => {
                        observed_relationships.push(("parent", live_context.clone()));
                    }
                }
            }

            for (_, context) in &observed_relationships {
                if let Some(context) = context {
                    let remote = context.as_remote_parent();
                    let remote_span = remote.span();
                    prop_assert_eq!(remote_span.span_context().trace_id(), TraceId::from_bytes(trace_id));
                    prop_assert_eq!(remote_span.span_context().span_id(), SpanId::from_bytes(span_id));
                }
            }
            let authoritative_history = serde_json::to_vec(&durable_ids).expect("durable ids serialize");
            let transient = serde_json::to_vec(&initial).expect("trace context serializes");
            prop_assert!(!authoritative_history.windows(trace_id.len()).any(|window| window == trace_id));
            prop_assert!(!String::from_utf8_lossy(&authoritative_history).contains("trace_state"));
            prop_assert_ne!(authoritative_history, transient);
            prop_assert_eq!(durable_ids["workflow_id"].as_str(), Some(workflow_id.as_str()));
            prop_assert_eq!(durable_ids["run_id"].as_str(), Some(run_id.as_str()));
            prop_assert_eq!(durable_ids["activity_id"].as_str(), Some(activity_id.as_str()));
            prop_assert_eq!(durable_ids["attempt"].as_u64(), Some(u64::from(attempt)));
        }
    }

    #[derive(Default)]
    struct HostCarrier(HashMap<String, String>);

    impl Injector for HostCarrier {
        fn set(&mut self, key: &str, value: String) {
            self.0.insert(key.to_owned(), value);
        }
    }

    impl Extractor for HostCarrier {
        fn get(&self, key: &str) -> Option<&str> {
            self.0.get(key).map(String::as_str)
        }

        fn keys(&self) -> Vec<&str> {
            self.0.keys().map(String::as_str).collect()
        }
    }

    #[test]
    fn host_owned_provider_mcp_and_handoff_carriers_compose() {
        let context = ChannelTraceContext {
            trace_id: [7; 16],
            span_id: [9; 8],
            trace_flags: TraceFlags::SAMPLED.to_u8(),
            trace_state: "host=fixture".to_owned(),
        };
        let propagator = TraceContextPropagator::new();
        let stable_ids = [
            ("tokeira.workflow_id", "workflow-fixture"),
            ("tokeira.run_id", "run-fixture"),
            ("tokeira.activity_id", "activity-fixture"),
        ];

        let mut provider = HostCarrier::default();
        propagator.inject_context(&context.as_remote_parent(), &mut provider);
        let provider_context = propagator.extract(&provider);
        let mut mcp_tool = HostCarrier::default();
        propagator.inject_context(&provider_context, &mut mcp_tool);
        let tool_context = propagator.extract(&mcp_tool);
        let mut handoff = HostCarrier::default();
        propagator.inject_context(&tool_context, &mut handoff);
        for (key, value) in stable_ids {
            handoff.0.insert(key.to_owned(), value.to_owned());
        }

        let extracted = propagator.extract(&handoff);
        let extracted_span = extracted.span();
        assert_eq!(
            extracted_span.span_context().trace_id(),
            TraceId::from_bytes(context.trace_id)
        );
        assert_eq!(
            extracted_span.span_context().span_id(),
            SpanId::from_bytes(context.span_id)
        );
        assert_eq!(
            handoff.0.get("tokeira.workflow_id").map(String::as_str),
            Some("workflow-fixture")
        );
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
