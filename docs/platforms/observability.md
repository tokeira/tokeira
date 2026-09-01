# Production Observability

Tokeira defines the telemetry exposed by its processes; each deployment
platform owns how that telemetry is collected, stored, queried, and presented.
There is no platform-independent requirement to deploy Alloy, Mimir, Loki,
Grafana, or any other observability stack. A platform may declare those
components, choose different ones, or expose only the process endpoints.

The same boundary governs operator checks. A platform may declare an
observability-check capability alongside its other operational capabilities.
The platform owns the check's categories and evidence because only it knows
which observability resources belong to the deployment.

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

OTLP trace export is configurable. Metrics are exposed in Prometheus format;
the selected platform decides whether and how to collect them.

## Compose Deployment

The Compose platform chooses Mimir, Loki, Grafana, and Alloy. Its generated
configuration includes:

- Alloy scrape jobs for Tokeira processes and infrastructure services.
- Loki forwarding for logs.
- Grafana dashboard provisioning.
- Mimir alert rule provisioning.

Dashboard JSON lives under `platforms/compose/observability/dashboards/`. Alert
rules live under `platforms/compose/observability/alerts/`. The Compose
generator discovers these
artifacts and renders them into the managed observability config tree.

## ECS Deployment

The legacy ECS platform services expose process observability endpoints inside
the private network. Each task gets observability environment values for
service, cluster, deployment, metrics bind address, and JSON logging. Alloy uses
task-scoped Docker discovery for log collection and local metrics scrape
configuration for each service.

Dashboard and alert artifacts are included in the ECS observability provisioning
resources so Grafana and Mimir can be hydrated without public endpoints.

## Smoke Check

Use:

```bash
tkr observability check
```

For a definition-backed deployment, `tkr` forwards this read-only command to
the deployment's bound `tkp`. The provisioner admits the recorded definition,
realizes its desired resources in memory, and delegates the entire check to the
capability in that platform's `PlatformDeclaration`. The framework prescribes
neither an observability stack nor common check categories. A platform that
does not declare the capability reports the command as not applicable.

The framework does not acquire the operation lock, invoke provisioner gates,
contact providers, or write deployment files while dispatching the check. Each
platform implementation must likewise remain read-only.

The current Compose capability validates Compose's own rendered Alloy scrape
jobs, Grafana dashboards, and Mimir alert rules. Those checks describe Compose,
not a requirement on other platforms. Legacy `deployment.toml` deployments
continue to use their existing in-process local/ECS behavior.

To validate one Grafana dashboard without selecting a deployment or assuming
a platform stack, select Grafana-only mode and pass the dashboard JSON file
itself:

```bash
tkr observability check --grafana --path /path/to/dashboard.json
```

This explicit mode is independent of any selected platform. It runs
`DashboardValidator` over that file and reports a single
`grafana-dashboard` `PASS` result. It does not run the Alloy or alert-rule
checks, report live-backend reachability, or admit a deployment. `--grafana`
and `--path` require each other; `--path` is not a generic rendered-stack input.

Per-deployment checks complement platform-owned unit tests. A platform can use
unit tests to validate its shipped sources and its declared capability to
validate the deployment-specific realized result.

## Grafana Dashboards

These conventions apply only when a platform chooses Grafana or when an
operator explicitly uses `--grafana`. They are not a requirement to deploy
Grafana.

The dashboards currently shipped by Compose cover:

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

## Compose Alerts

Alert rules are stored in
`platforms/compose/observability/alerts/observability-alerts.yaml`. Each alert
includes bounded severity and service labels plus a summary and description.
Runbook URLs are intentionally not emitted until the production-observability
surface defines a stable public documentation home.

## Implementer Notes

- Add every new metric to a manifest before using it.
- Keep labels bounded; never add workflow/run/request/trace identifiers as
  metric labels.
- Use typed label enums instead of ad-hoc strings on hot paths.
- Use manual spans on hot or cancellable async paths.
- When a platform ships dashboards or alerts, keep its validation tests beside
  that platform's content and declare any deployment-specific check through
  `PlatformDeclaration`.
- Keep platform defaults private-network friendly. Public observability
  endpoints are not required by the process telemetry contract.
