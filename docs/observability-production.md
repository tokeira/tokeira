# Production Observability

Tokeira's production observability model is process-local telemetry plus
platform-level collection. Each production process exposes its own HTTP
observability endpoints; Alloy scrapes or collects from those endpoints and
forwards the data to Mimir and Loki. Tokeira does not aggregate telemetry inside
the application process. Aggregation belongs to the observability stack.

## Process Endpoints

Each production process exposes:

- `/metrics`: Prometheus exposition for process metrics.
- `/healthz`: liveness only. This endpoint does not check external dependencies.
- `/readyz`: readiness as JSON. Required dependency or loop failures return a
  non-2xx response.

Projection workers currently run embedded in `tokeirad`, so projection metrics
are exposed through the `tokeirad` endpoint and are identified by the
`tokeira_projection_*` metric prefix. A standalone projection process can add
its own endpoint later without changing metric names.

## Metrics

Every application metric must be declared in a manifest before it is used by a
dashboard or alert. The manifest records the metric type, unit, owner,
description, labels, and label cardinality classification.

Metric names follow these conventions:

- Counters end in `_total`.
- Duration histograms end in `_seconds`.
- Ratio gauges end in `_ratio` unless explicitly exempted.
- Existing emitted metric names are authoritative. Do not rename production
  metrics without updating dashboards, alerts, autoscaler queries, and smoke
  tests in the same change.

Metric labels must be bounded. Use enum-backed helpers where possible.
Configuration-bounded labels such as `namespace`, `task_queue`, `partition_id`,
`shard_id`, and `lane_id` are allowed only when their bounds are documented.
Never use workflow IDs, run IDs, request IDs, trace IDs, raw SQL, raw error
messages, node endpoints, or ECS task ARNs as metric labels. Those values belong
in structured logs or spans.

## Logs

Production logs are JSON. Local development may continue to use text logs.
Production JSON records include service, cluster, deployment, and node/task
identity where available. Span-scoped logs include trace correlation fields when
the tracing layer is active.

Sensitive fields are redacted before they are exposed through logs or config
snapshots. Treat passwords, tokens, credentials, private keys, authorization
headers, and credential-bearing connection strings as sensitive by default.

## Traces

HTTP/gRPC boundaries use W3C TraceContext. Runtime lane dispatch is channel
based, so it uses correlation attributes instead of holding a parent span across
an async boundary. The dispatch envelope carries `origin_trace_id` and
`origin_span_id`; the lane processing span records those values as attributes.
This avoids the span lifecycle coupling that previously caused panics under
high-concurrency cancellation.

Hot or cancellable paths should use explicit manual spans around stable
boundaries. Avoid broad `#[instrument]` on storage commit paths, lane execution,
and other high-throughput async futures unless the span lifecycle has been
audited.

OTLP trace export is configurable. OTLP metrics push is Phase 2; Phase 1 relies
on Prometheus scrape coverage and Alloy/Mimir ingestion.

## Compose Deployment

Compose provisions Mimir, Loki, Grafana, and Alloy. Generated configuration
includes:

- Alloy scrape jobs for Tokeira processes and infrastructure services.
- Loki forwarding for logs.
- Grafana dashboard provisioning.
- Mimir alert rule provisioning.

Dashboard JSON lives under `platforms/compose/dashboards/`. Alert rules live
under `platforms/compose/alerts/`. The Compose generator discovers these
artifacts and renders them into the managed observability config tree.

## ECS Deployment

ECS services expose process observability endpoints inside the private network.
Each task gets observability environment values for service, cluster,
deployment, metrics bind address, and JSON logging. Alloy uses task-scoped
Docker discovery for log collection and local metrics scrape configuration for
each service.

Dashboard and alert artifacts are included in the ECS observability provisioning
resources so Grafana and Mimir can be hydrated without public endpoints.

## Smoke Check

Use:

```bash
tkr observability check
```

The command validates generated observability configuration for the selected
deployment. It checks scrape configuration, dashboard rendering, alert rule
rendering, and ECS observability artifact inclusion where applicable. Live
backend checks require the relevant deployment endpoints to be reachable, for
example through `tkr port-forward grafana`, `tkr port-forward mimir`, or private
network access.

## Dashboards

Provisioned dashboard families include:

- Broker/runtime health.
- gRPC edge health.
- Storage/projection health.
- DSQL connection health.
- OCC contention.
- Placement controller.
- Autoscaler.
- Projection workers.
- Infrastructure health.

Dashboards must use declared metric names, smooth line interpolation, explicit
units, descriptions, meaningful legends, and the `$datasource` template
variable. Do not put high-cardinality labels in legends.

## Alerts and Runbooks

Alert rules are stored in `platforms/compose/alerts/observability-alerts.yaml`.
Each alert includes severity, service, summary, description, and a runbook URL.
Runbooks live under `docs/runbooks/observability/` and cover first dashboard,
first PromQL/log queries, safe remediation, escalation, and related alerts.

When adding a new alert, add its runbook in the same change and update the alert
validation tests.

## Implementer Notes

- Add every new metric to a manifest before using it.
- Keep labels bounded; never add workflow/run/request/trace identifiers as
  metric labels.
- Use typed label enums instead of ad-hoc strings on hot paths.
- Use manual spans on hot or cancellable async paths.
- Update dashboard, alert, and runbook validation tests when adding telemetry
  artifacts.
- Keep production defaults private-network friendly. Public observability
  endpoints are not required for Compose or ECS operation.

