//! Structured log formatting with trace and process correlation.
//!
//! The formatter wraps `tracing_subscriber`'s normal event formatter rather than
//! replacing it. That keeps formatting behavior compatible with the upstream
//! subscriber while injecting the fields operators need to pivot between logs,
//! traces, and deployment metadata.

use std::fmt;

use opentelemetry::trace::TraceContextExt;
use serde_json::Value;
use tracing::{Event, Span, Subscriber};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::{
    fmt::{FmtContext, FormatEvent, FormatFields, format::Writer},
    registry::LookupSpan,
};

use crate::ProcessObservabilityConfig;

/// Formatter wrapper that injects OpenTelemetry trace/span IDs into logs.
///
/// Text output prepends correlation fields when a valid span context exists.
/// JSON output parses the inner JSON event and appends correlation/resource
/// fields so log backends can index them as first-class attributes.
#[derive(Debug)]
pub struct CorrelationFormat<E> {
    inner: E,
    json: bool,
    resource: Option<LogResourceFields>,
}

#[derive(Clone, Debug)]
pub(crate) struct LogResourceFields {
    service: &'static str,
    cluster: String,
    deployment: String,
    node_id: Option<String>,
    task_id: Option<String>,
}

impl<E> CorrelationFormat<E> {
    /// Wrap a text event formatter.
    pub fn text(inner: E) -> Self {
        Self {
            inner,
            json: false,
            resource: None,
        }
    }

    /// Wrap a JSON event formatter.
    pub fn json(inner: E) -> Self {
        Self {
            inner,
            json: true,
            resource: None,
        }
    }

    pub(crate) fn with_resource(mut self, resource: LogResourceFields) -> Self {
        self.resource = Some(resource);
        self
    }
}

impl LogResourceFields {
    pub(crate) fn from_config(config: &ProcessObservabilityConfig) -> Self {
        Self {
            service: config.service_name.as_str(),
            cluster: config.cluster_name.clone(),
            deployment: config.deployment_name.clone(),
            node_id: config.node_id.clone(),
            task_id: config.task_id.clone(),
        }
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
            write_json_event(
                &mut writer,
                rendered.trim_end(),
                correlation,
                self.resource.as_ref(),
            )
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
    resource: Option<&LogResourceFields>,
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

    if let Some(resource) = resource {
        value.insert(
            "service".to_string(),
            Value::String(resource.service.to_string()),
        );
        value.insert(
            "cluster".to_string(),
            Value::String(resource.cluster.clone()),
        );
        value.insert(
            "deployment".to_string(),
            Value::String(resource.deployment.clone()),
        );
        if let Some(node_id) = &resource.node_id {
            value.insert("node_id".to_string(), Value::String(node_id.clone()));
        }
        if let Some(task_id) = &resource.task_id {
            value.insert("task_id".to_string(), Value::String(task_id.clone()));
        }
    }

    let output = serde_json::to_string(&value).map_err(|_| fmt::Error)?;
    writer.write_str(&output)?;
    writer.write_char('\n')?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_formatter_injects_process_resource_fields() {
        let mut rendered = String::new();
        let mut output = Writer::new(&mut rendered);
        let resource = LogResourceFields {
            service: "tokeirad",
            cluster: "test-cluster".to_string(),
            deployment: "test-deployment".to_string(),
            node_id: Some("node-a".to_string()),
            task_id: Some("task-a".to_string()),
        };

        write_json_event(
            &mut output,
            r#"{"timestamp":"2026-01-01T00:00:00Z","level":"INFO","target":"test","fields":{"message":"hello"}}"#,
            None,
            Some(&resource),
        )
        .unwrap();

        let json: Value = serde_json::from_str(rendered.trim()).unwrap();
        assert_eq!(json["service"], "tokeirad");
        assert_eq!(json["cluster"], "test-cluster");
        assert_eq!(json["deployment"], "test-deployment");
        assert_eq!(json["node_id"], "node-a");
        assert_eq!(json["task_id"], "task-a");
        assert_eq!(json["level"], "INFO");
        assert_eq!(json["target"], "test");
    }

    #[test]
    fn json_formatter_injects_trace_correlation_fields() {
        let mut rendered = String::new();
        let mut output = Writer::new(&mut rendered);

        write_json_event(
            &mut output,
            r#"{"timestamp":"2026-01-01T00:00:00Z","level":"INFO","target":"test"}"#,
            Some((
                "0123456789abcdef0123456789abcdef".to_string(),
                "0123456789abcdef".to_string(),
            )),
            None,
        )
        .unwrap();

        let json: Value = serde_json::from_str(rendered.trim()).unwrap();
        assert_eq!(json["trace_id"], "0123456789abcdef0123456789abcdef");
        assert_eq!(json["span_id"], "0123456789abcdef");
    }
}
