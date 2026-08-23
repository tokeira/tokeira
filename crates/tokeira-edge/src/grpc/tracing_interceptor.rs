use std::future::Future;

use http::HeaderMap;
use opentelemetry::{
    Context as OtelContext,
    propagation::{Extractor, TextMapPropagator},
};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tracing::{Instrument, field};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::request_id::REQUEST_ID_HEADER;

struct HeaderExtractor<'a> {
    headers: &'a HeaderMap,
}

impl Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.headers.get(key).and_then(|value| value.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.headers.keys().map(|name| name.as_str()).collect()
    }
}

pub fn extract_trace_context(headers: &HeaderMap) -> OtelContext {
    TraceContextPropagator::new().extract(&HeaderExtractor { headers })
}

pub async fn instrument_grpc_call<T, F>(
    headers: &HeaderMap,
    method: &str,
    namespace: Option<&str>,
    workflow_id: Option<&str>,
    fut: F,
) -> T
where
    F: Future<Output = T>,
{
    let parent = extract_trace_context(headers);
    let span_name = format!("grpc.{method}");
    let span = tracing::span!(
        tracing::Level::INFO,
        "grpc.request",
        otel.name = span_name.as_str(),
        rpc.system = "grpc",
        rpc.service = "tokeira-edge",
        rpc.method = method,
        server.address = field::Empty,
        tokeira.namespace = field::Empty,
        tokeira.request_id = field::Empty,
        tokeira.workflow_id = field::Empty,
        method = method,
        namespace = field::Empty,
        workflow_id = field::Empty,
    );
    let _ = span.set_parent(parent);
    if let Some(request_id) = headers
        .get(REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
    {
        span.record("tokeira.request_id", request_id);
    }
    if let Some(namespace) = namespace {
        span.record("namespace", namespace);
        span.record("tokeira.namespace", namespace);
    }
    if let Some(workflow_id) = workflow_id {
        span.record("workflow_id", workflow_id);
        span.record("tokeira.workflow_id", workflow_id);
    }
    fut.instrument(span).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    use opentelemetry::{
        propagation::{Injector, TextMapPropagator},
        trace::{SpanId, TraceContextExt, TraceId, TracerProvider},
    };
    use opentelemetry_sdk::{
        error::OTelSdkResult,
        trace::{Sampler, SpanData, SpanExporter},
    };
    use proptest::{prelude::*, test_runner::TestRunner};
    use tracing::{
        Subscriber,
        field::{Field, Visit},
        span::{Attributes, Id},
    };
    use tracing_subscriber::{
        Layer,
        layer::{Context, SubscriberExt},
        registry::LookupSpan,
    };

    #[derive(Clone, Default)]
    struct SpanCapture(Arc<Mutex<Vec<(String, HashMap<String, String>)>>>);

    struct FieldRecorder {
        values: HashMap<String, String>,
    }

    impl Visit for FieldRecorder {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.values
                .insert(field.name().to_string(), format!("{value:?}"));
        }

        fn record_str(&mut self, field: &Field, value: &str) {
            self.values
                .insert(field.name().to_string(), value.to_string());
        }
    }

    impl<S> Layer<S> for SpanCapture
    where
        S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    {
        fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
            let mut recorder = FieldRecorder {
                values: HashMap::new(),
            };
            attrs.record(&mut recorder);
            if let Some(span) = ctx.span(id) {
                self.0
                    .lock()
                    .unwrap()
                    .push((span.metadata().name().to_string(), recorder.values));
            }
        }

        fn on_record(&self, id: &Id, values: &tracing::span::Record<'_>, ctx: Context<'_, S>) {
            let mut recorder = FieldRecorder {
                values: HashMap::new(),
            };
            values.record(&mut recorder);
            if let Some(span) = ctx.span(id) {
                let span_name = span.metadata().name();
                let mut captured = self.0.lock().unwrap();
                if let Some((_, fields)) = captured
                    .iter_mut()
                    .rev()
                    .find(|(name, _)| name == span_name)
                {
                    fields.extend(recorder.values);
                }
            }
        }
    }

    struct HeaderInjector {
        values: HashMap<String, String>,
    }

    #[derive(Clone, Debug, Default)]
    struct TestSpanExporter(Arc<Mutex<Vec<SpanData>>>);

    impl TestSpanExporter {
        fn finished_spans(&self) -> Vec<SpanData> {
            self.0.lock().unwrap().clone()
        }
    }

    impl SpanExporter for TestSpanExporter {
        async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
            self.0.lock().unwrap().extend(batch);
            Ok(())
        }
    }

    impl Injector for HeaderInjector {
        fn set(&mut self, key: &str, value: String) {
            self.values.insert(key.to_string(), value);
        }
    }

    fn hex<const N: usize>(bytes: [u8; N]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    #[test]
    fn service_override_preserves_w3c_parentage() {
        let exporter = TestSpanExporter::default();
        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            // Export every generated child so the property can inspect both
            // sampled and unsampled incoming W3C flags. Sampling policy is a
            // host choice and is not what this boundary test exercises.
            .with_sampler(Sampler::AlwaysOn)
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("service-override-property");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
        let dispatch = tracing::Dispatch::new(subscriber);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("property runtime");
        let mut runner = TestRunner::new(ProptestConfig::with_cases(128));

        // Feature: managed-embedded-dsql, Property 15: service_override preserves W3C parentage
        runner
            .run(
                &(
                    0u8..3,
                    any::<[u8; 16]>(),
                    any::<[u8; 8]>(),
                    any::<bool>(),
                    "[a-z]{1,8}",
                    "[a-z0-9]{1,12}",
                ),
                |(kind, trace_id, span_id, sampled, state_key, state_value)| {
                    prop_assume!(trace_id.iter().any(|byte| *byte != 0));
                    prop_assume!(span_id.iter().any(|byte| *byte != 0));
                    let flags = if sampled { 0x01 } else { 0x00 };
                    let mut headers = HeaderMap::new();
                    if kind == 0 {
                        let traceparent =
                            format!("00-{}-{}-{flags:02x}", hex(trace_id), hex(span_id));
                        headers.insert("traceparent", traceparent.parse().expect("valid header"));
                        let tracestate = format!("{state_key}={state_value}");
                        headers.insert("tracestate", tracestate.parse().expect("valid header"));
                    } else if kind == 1 {
                        headers.insert("traceparent", "malformed".parse().expect("literal header"));
                    }

                    let extracted = extract_trace_context(&headers);
                    let extracted_span = extracted.span();
                    let extracted_context = extracted_span.span_context();
                    prop_assert_eq!(extracted_context.is_valid(), kind == 0);

                    let result = tracing::dispatcher::with_default(&dispatch, || {
                        runtime.block_on(instrument_grpc_call(
                            &headers,
                            "test_operation",
                            Some("default"),
                            Some("workflow-a"),
                            async { 41_u64 },
                        ))
                    });
                    prop_assert_eq!(result, 41);

                    let spans = exporter.finished_spans();
                    let recorded = spans.last().expect("server span exported");
                    if kind == 0 {
                        prop_assert_eq!(
                            recorded.span_context.trace_id(),
                            TraceId::from_bytes(trace_id)
                        );
                        prop_assert_eq!(recorded.parent_span_id, SpanId::from_bytes(span_id));
                        prop_assert!(recorded.parent_span_is_remote);
                        prop_assert_eq!(
                            recorded.span_context.trace_state().header(),
                            format!("{state_key}={state_value}")
                        );

                        let mut injector = HeaderInjector {
                            values: HashMap::new(),
                        };
                        TraceContextPropagator::new().inject_context(&extracted, &mut injector);
                        let reinjected = injector
                            .values
                            .get("traceparent")
                            .cloned()
                            .unwrap_or_default();
                        prop_assert!(reinjected.contains(&hex(trace_id)));
                        prop_assert!(reinjected.contains(&hex(span_id)));
                    } else {
                        prop_assert_eq!(recorded.parent_span_id, SpanId::INVALID);
                        prop_assert!(!recorded.parent_span_is_remote);
                    }
                    Ok(())
                },
            )
            .expect("W3C parentage property");
    }

    #[tokio::test]
    async fn creates_root_span_without_traceparent() {
        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder().build();
        let tracer = provider.tracer("test");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
        let dispatch = tracing::Dispatch::new(subscriber);
        let _guard = tracing::dispatcher::set_default(&dispatch);

        let observed = Arc::new(Mutex::new(None));
        let observed_clone = observed.clone();
        let headers = HeaderMap::new();
        instrument_grpc_call(
            &headers,
            "poll_workflow_task_queue",
            Some("default"),
            None,
            async move {
                *observed_clone.lock().unwrap() = Some(tracing::Span::current().id().is_some());
            },
        )
        .await;

        assert_eq!(*observed.lock().unwrap(), Some(true));
    }

    #[tokio::test]
    async fn records_grpc_method_name_in_otel_span_name() {
        let capture = SpanCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let dispatch = tracing::Dispatch::new(subscriber);
        let _guard = tracing::dispatcher::set_default(&dispatch);

        let headers = HeaderMap::new();
        instrument_grpc_call(
            &headers,
            "poll_workflow_task_queue",
            Some("default"),
            None,
            async {},
        )
        .await;

        let spans = capture.0.lock().unwrap();
        assert!(spans.iter().any(|(name, fields)| {
            name == "grpc.request"
                && fields.get("otel.name") == Some(&"grpc.poll_workflow_task_queue".to_string())
        }));
    }

    #[tokio::test]
    async fn records_standard_rpc_and_tokeira_request_attributes() {
        let capture = SpanCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let dispatch = tracing::Dispatch::new(subscriber);
        let _guard = tracing::dispatcher::set_default(&dispatch);

        let mut headers = HeaderMap::new();
        headers.insert(REQUEST_ID_HEADER, "req-123".parse().unwrap());
        instrument_grpc_call(
            &headers,
            "start_workflow_execution",
            Some("default"),
            Some("workflow-a"),
            async {},
        )
        .await;

        let spans = capture.0.lock().unwrap();
        let (_, fields) = spans
            .iter()
            .find(|(name, _)| name == "grpc.request")
            .expect("gRPC root span should be emitted");
        assert_eq!(fields.get("rpc.system"), Some(&"grpc".to_string()));
        assert_eq!(fields.get("rpc.service"), Some(&"tokeira-edge".to_string()));
        assert_eq!(
            fields.get("rpc.method"),
            Some(&"start_workflow_execution".to_string())
        );
        assert_eq!(
            fields.get("tokeira.namespace"),
            Some(&"default".to_string())
        );
        assert_eq!(
            fields.get("tokeira.request_id"),
            Some(&"req-123".to_string())
        );
    }
}
