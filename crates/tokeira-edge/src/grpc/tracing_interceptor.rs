use std::future::Future;

use http::HeaderMap;
use opentelemetry::{
    Context as OtelContext,
    propagation::{Extractor, TextMapPropagator},
};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tracing::{Instrument, field};
use tracing_opentelemetry::OpenTelemetrySpanExt;

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
        method = method,
        namespace = field::Empty,
        workflow_id = field::Empty,
    );
    let _ = span.set_parent(parent);
    if let Some(namespace) = namespace {
        span.record("namespace", namespace);
    }
    if let Some(workflow_id) = workflow_id {
        span.record("workflow_id", workflow_id);
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
        trace::{TraceContextExt, TracerProvider},
    };
    use proptest::prelude::*;
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
    }

    struct HeaderInjector {
        values: HashMap<String, String>,
    }

    impl Injector for HeaderInjector {
        fn set(&mut self, key: &str, value: String) {
            self.values.insert(key.to_string(), value);
        }
    }

    fn hex<const N: usize>(bytes: [u8; N]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    proptest! {
        #[test]
        fn property_trace_context_round_trip(trace_id in any::<[u8; 16]>(), span_id in any::<[u8; 8]>(), sampled in any::<bool>()) {
            prop_assume!(trace_id.iter().any(|byte| *byte != 0));
            prop_assume!(span_id.iter().any(|byte| *byte != 0));
            let flags = if sampled { 0x01 } else { 0x00 };
            let mut headers = HeaderMap::new();
            let traceparent = format!("00-{}-{}-{:02x}", hex(trace_id), hex(span_id), flags);
            headers.insert("traceparent", traceparent.parse().unwrap());

            let extracted = extract_trace_context(&headers);
            let extracted_span = extracted.span();
            let extracted_context = extracted_span.span_context();
            prop_assert!(extracted_context.is_valid());
            prop_assert_eq!(extracted_context.trace_id().to_string(), hex(trace_id));
            prop_assert_eq!(extracted_context.span_id().to_string(), hex(span_id));

            let mut injector = HeaderInjector { values: HashMap::new() };
            TraceContextPropagator::new().inject_context(&extracted, &mut injector);
            let reinjected = injector.values.get("traceparent").cloned().unwrap_or_default();
            prop_assert!(reinjected.contains(&hex(trace_id)));
            prop_assert!(reinjected.contains(&hex(span_id)));
        }
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
}
