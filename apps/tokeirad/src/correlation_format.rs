use std::fmt;

use opentelemetry::trace::TraceContextExt;
use serde_json::Value;
use tracing::{Event, Span, Subscriber};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::{
    fmt::{FmtContext, FormatEvent, FormatFields, format::Writer},
    registry::LookupSpan,
};

/// Formatter wrapper that augments emitted log records with the active
/// OpenTelemetry trace/span identifiers when a traced span is in scope.
pub struct CorrelationFormat<E> {
    inner: E,
    json: bool,
}

impl<E> CorrelationFormat<E> {
    pub fn text(inner: E) -> Self {
        Self { inner, json: false }
    }

    pub fn json(inner: E) -> Self {
        Self { inner, json: true }
    }
}

impl<S, N, E> FormatEvent<S, N> for CorrelationFormat<E>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'writer> FormatFields<'writer> + 'static,
    E: FormatEvent<S, N>,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let mut rendered = String::new();
        self.inner
            .format_event(ctx, Writer::new(&mut rendered), event)?;

        let correlation = current_correlation_ids();
        if self.json {
            write_json_event(&mut writer, rendered.trim_end(), correlation)
        } else {
            if let Some((trace_id, span_id)) = correlation {
                write!(writer, "trace_id={trace_id} span_id={span_id} ")?;
            }
            writer.write_str(&rendered)
        }
    }
}

fn current_correlation_ids() -> Option<(String, String)> {
    let context = Span::current().context();
    let span = context.span();
    let span_context = span.span_context();
    if span_context.is_valid() {
        Some((
            span_context.trace_id().to_string(),
            span_context.span_id().to_string(),
        ))
    } else {
        None
    }
}

fn write_json_event(
    writer: &mut Writer<'_>,
    rendered: &str,
    correlation: Option<(String, String)>,
) -> fmt::Result {
    let mut value = match serde_json::from_str::<Value>(rendered) {
        Ok(Value::Object(object)) => object,
        _ => {
            writer.write_str(rendered)?;
            writer.write_char('\n')?;
            return Ok(());
        }
    };

    if let Some((trace_id, span_id)) = correlation {
        value.insert("trace_id".to_string(), Value::String(trace_id));
        value.insert("span_id".to_string(), Value::String(span_id));
    }

    let output = serde_json::to_string(&value).map_err(|_| fmt::Error)?;
    writer.write_str(&output)?;
    writer.write_char('\n')?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io,
        sync::{Arc, Mutex},
    };

    use opentelemetry::trace::TracerProvider as _;
    use proptest::prelude::*;
    use tracing_subscriber::{
        fmt::{self, MakeWriter},
        layer::SubscriberExt,
    };

    #[derive(Clone, Default)]
    struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

    struct BufferWriter(Arc<Mutex<Vec<u8>>>);

    impl io::Write for BufferWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for SharedBuffer {
        type Writer = BufferWriter;

        fn make_writer(&'a self) -> Self::Writer {
            BufferWriter(self.0.clone())
        }
    }

    impl SharedBuffer {
        fn output(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
    }

    fn install_test_subscriber_json(buffer: SharedBuffer) -> impl tracing::Subscriber {
        tracing_subscriber::registry().with(
            fmt::layer()
                .with_writer(buffer)
                .fmt_fields(tracing_subscriber::fmt::format::JsonFields::new())
                .event_format(CorrelationFormat::json(
                    tracing_subscriber::fmt::format().json(),
                )),
        )
    }

    fn install_test_subscriber_text(buffer: SharedBuffer) -> impl tracing::Subscriber {
        tracing_subscriber::registry().with(
            fmt::layer()
                .with_writer(buffer)
                .event_format(CorrelationFormat::text(tracing_subscriber::fmt::format())),
        )
    }

    proptest! {
        #[test]
        fn property_json_log_record_validity(message in ".*") {
            let buffer = SharedBuffer::default();
            let subscriber = install_test_subscriber_json(buffer.clone());
            tracing::subscriber::with_default(subscriber, || {
                tracing::info!(message = %message, "property-log");
            });

            let line = buffer.output();
            let parsed: Value = serde_json::from_str(line.trim()).unwrap();
            let object = parsed.as_object().unwrap();
            prop_assert!(object.contains_key("timestamp"));
            prop_assert!(object.contains_key("level"));
            prop_assert!(object.contains_key("target"));
            prop_assert!(
                object.contains_key("message") ||
                object
                    .get("fields")
                    .and_then(Value::as_object)
                    .is_some_and(|fields| fields.contains_key("message"))
            );
        }
    }

    #[test]
    fn omits_trace_fields_without_active_span() {
        let buffer = SharedBuffer::default();
        let subscriber = install_test_subscriber_text(buffer.clone());
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!("outside-span");
        });

        let output = buffer.output();
        assert!(!output.contains("trace_id="));
        assert!(!output.contains("span_id="));
    }

    #[test]
    fn json_formatter_remains_valid_inside_active_span() {
        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder().build();
        let tracer = provider.tracer("test");
        let buffer = SharedBuffer::default();
        let subscriber = tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(tracer))
            .with(
                fmt::layer()
                    .with_writer(buffer.clone())
                    .fmt_fields(tracing_subscriber::fmt::format::JsonFields::new())
                    .event_format(CorrelationFormat::json(
                        tracing_subscriber::fmt::format().json(),
                    )),
            );

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("demo");
            let _guard = span.enter();
            tracing::info!("inside-span");
        });

        let parsed: Value = serde_json::from_str(buffer.output().trim()).unwrap();
        let object = parsed.as_object().unwrap();
        assert!(object.contains_key("timestamp"));
        assert!(object.contains_key("level"));
    }
}
