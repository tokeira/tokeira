# Requirements Document

## Introduction

Production-facing observability for the full Tokeira ECS deployment surface: tokeirad (edge, runtime, storage), tokeira-controller (placement), tokeira-autoscaler (scaling), and tokeira-projection (visibility workers). Covers the unified metric export pipeline (Prometheus scrape endpoint and OTLP push), DSQL-specific operational metrics (OCC conflicts, retry histograms, reservoir depth, rate-limiter tokens, class-budget saturation), migration event tracking, connection-leak detection, distributed trace attributes surfacing the full gRPC → edge → runtime → kernel → storage path, controller placement metrics, autoscaler decision metrics, projection worker metrics, dashboard provisioning, and alerting rules.

This spec builds on two completed specs:
- `observability-foundation` — established the `metrics` crate recording API, Prometheus endpoint on `:9090`, `METRIC_NAMES` manifests, and `validate_metric_name()` convention.
- `dsql-observability-metrics` — defined 37 requirements across 11 phases for DSQL-specific instrumentation (operation latency, OCC conflicts, reservoir, rate limiter, class budgets, projection, per-shard distribution, error classification, query decomposition).

The `dsql-observability-metrics` spec should be treated as the authoritative reference for DSQL metric names, label conventions, and dashboard panel styling. This spec extends that work to cover the full deployment footprint.

### Known Instrumentation Constraint

A `tracing-subscriber` panic (`tried to clone a span that already closed`) has been observed under high concurrency in DSQL/runtime paths. The suspected cause is a race around `#[instrument]` spans on async paths under cancellation or heavy task churn. Until this is resolved:
- Prefer explicit span creation/recording around stable boundaries over `#[instrument]` on hot/cancellable async functions.
- Avoid adding new `#[instrument]` attributes to high-concurrency storage commit paths or lane execution paths.
- Consider upgrading `tracing-subscriber` if a post-0.3.23 fix exists.
- The trace attributes requirement (Requirement 9) must account for this constraint — use manual span creation where `#[instrument]` is unsafe.

## Glossary

- **Tokeirad**: The tokeira server binary that hosts the gRPC edge, runtime, and storage layers.
- **Export_Pipeline**: The subsystem responsible for making metrics available to external collectors via Prometheus scrape and OTLP push.
- **Prometheus_Endpoint**: The HTTP `/metrics` endpoint on port 9090 that exposes metrics in Prometheus exposition format.
- **OTLP_Exporter**: The OpenTelemetry Protocol exporter that pushes metrics to a remote collector (gRPC or HTTP).
- **OCC_Conflict**: An optimistic concurrency control conflict (SQLSTATE 40001) raised by Aurora DSQL when two transactions race on the same row.
- **Retry_Histogram**: A histogram metric recording the number of OCC retries per storage operation.
- **Migration_Event**: A structured log or metric emitted when a DSQL schema migration is applied, rolled back, or fails.
- **Connection_Leak_Detector**: A subsystem that identifies DSQL connections checked out from the reservoir but not returned within a configurable deadline.
- **Reservoir**: The channel-based connection buffer that pre-creates DSQL connections so checkout never blocks.
- **Rate_Limiter**: The token-bucket mechanism that enforces the DSQL cluster-wide 100 connections/second creation rate.
- **Class_Budget**: The per-DbClass connection permit allocation (Control 10%, Commit 50%, Read 20%, Projection 10%, Maintenance remainder).
- **Trace_Context**: The set of span attributes and propagation headers that link a request from gRPC ingress through edge, runtime, kernel, and storage layers.
- **Placement_Controller**: The `tokeira-controller` binary that manages shard-to-node assignment, routing snapshots, and drain coordination.
- **Autoscaler**: The `tokeira-autoscaler` binary that manages replica scaling (Loop A), runtime scale-out (Loop B), and runtime retirement (Loop C).
- **Dashboard**: A Grafana JSON model provisioned automatically that visualizes a coherent set of metrics.
- **Alert_Rule**: A Prometheus alerting rule (or Mimir ruler rule) that fires when a metric crosses a threshold for a sustained duration.
- **Alloy**: The Grafana telemetry collector that scrapes Prometheus endpoints and forwards to Mimir/Loki.
- **Mimir**: The Grafana-compatible long-term metrics store used in the compose and ECS observability stacks.
- **Dashboard**: A Grafana JSON model provisioned automatically that visualizes a coherent set of metrics.
- **Alert_Rule**: A Prometheus alerting rule (or Mimir ruler rule) that fires when a metric crosses a threshold for a sustained duration.
- **Alloy**: The Grafana telemetry collector that scrapes Prometheus endpoints and forwards to Mimir/Loki.
- **Mimir**: The Grafana-compatible long-term metrics store used in the compose and ECS observability stacks.

## Requirements

### Requirement 1: Prometheus Scrape Endpoint Completeness

**User Story:** As an operator, I want the Prometheus scrape endpoint to expose all registered metrics from every crate in the workspace, so that I can monitor tokeirad without missing signals.

#### Acceptance Criteria

1. THE Prometheus_Endpoint SHALL expose all metrics registered in `tokeira-runtime/src/metrics.rs`, `tokeira-storage/src/metrics.rs`, `tokeira-edge` metrics, and `tokeira-projection` metrics in a single scrape response.
2. WHEN a metric is recorded by any crate in the workspace, THE Prometheus_Endpoint SHALL include that metric in the next scrape response within one scrape interval.
3. THE Prometheus_Endpoint SHALL include a `tokeira_build_info` gauge with `version`, `commit`, and `rustc_version` labels.
4. THE Prometheus_Endpoint SHALL respond to GET requests on `/metrics` with content type `text/plain; version=0.0.4` and HTTP status 200.

### Requirement 2: OTLP Metrics Push Export

**User Story:** As an operator deploying to environments without a Prometheus scraper, I want tokeirad to push metrics via OTLP, so that I can collect metrics in push-based architectures.

#### Acceptance Criteria

1. WHEN `infrastructure.observability.otlp_enabled` is true, THE OTLP_Exporter SHALL push metric data to the configured `otlp_endpoint` using the configured protocol (gRPC or HTTP).
2. THE OTLP_Exporter SHALL export the same metric set available on the Prometheus_Endpoint.
3. IF the OTLP endpoint is unreachable, THEN THE OTLP_Exporter SHALL buffer metric batches in memory up to a configurable limit and retry with exponential backoff.
4. IF the buffer limit is exceeded, THEN THE OTLP_Exporter SHALL drop the oldest batch and increment a `tokeira_otlp_dropped_batches_total` counter.
5. WHILE OTLP export is enabled, THE OTLP_Exporter SHALL include resource attributes identifying the cluster name, node identity, and tokeirad version.

### Requirement 3: OCC Conflict Counters and Retry Histograms

**User Story:** As an operator, I want to see OCC conflict rates and retry distributions per storage operation, so that I can detect contention hotspots and tune shard placement.

#### Acceptance Criteria

1. WHEN an OCC conflict occurs, THE Storage_Layer SHALL increment `tokeira_storage_dsql_occ_conflict_total` with an `operation` label identifying the storage operation.
2. WHEN a storage operation completes after retries, THE Storage_Layer SHALL record the retry count in `tokeira_storage_dsql_commit_retries` histogram.
3. THE Storage_Layer SHALL record `tokeira_storage_dsql_retry_total` with `operation` and `outcome` labels distinguishing successful retries from exhausted retries.
4. WHEN the retry budget is exhausted, THE Storage_Layer SHALL increment `tokeira_storage_dsql_retry_total` with `outcome=exhausted`.

### Requirement 4: Migration Event Observability

**User Story:** As an operator, I want to know when schema migrations run, succeed, or fail, so that I can correlate deployment events with behavioral changes.

#### Acceptance Criteria

1. WHEN a DSQL schema migration is applied successfully, THE Migration_Event system SHALL emit a structured log at INFO level containing the migration filename, duration, and resulting schema version.
2. WHEN a DSQL schema migration fails, THE Migration_Event system SHALL emit a structured log at ERROR level containing the migration filename, error message, and the SQLSTATE code.
3. THE Migration_Event system SHALL increment a `tokeira_storage_migration_applied_total` counter with a `status` label (`success` or `failure`) for each migration attempt.
4. THE Migration_Event system SHALL record migration duration in a `tokeira_storage_migration_duration_seconds` histogram.

### Requirement 5: Connection Leak Detection

**User Story:** As an operator, I want to detect DSQL connections that are checked out but never returned, so that I can identify code paths that leak connections and prevent reservoir exhaustion.

#### Acceptance Criteria

1. WHEN a connection has been checked out from the Reservoir for longer than a configurable deadline (default 60 seconds), THE Connection_Leak_Detector SHALL emit a structured warning log containing the checkout duration, the DbClass, and a stack trace identifier of the checkout call site.
2. THE Connection_Leak_Detector SHALL increment a `tokeira_dsql_connection_leak_detected_total` counter with a `class` label each time a leak is detected.
3. THE Connection_Leak_Detector SHALL set a `tokeira_dsql_connection_leak_suspects` gauge reflecting the current number of connections exceeding the leak deadline.
4. IF a suspected leaked connection is eventually returned, THEN THE Connection_Leak_Detector SHALL decrement the suspects gauge and record the total checkout duration in a `tokeira_dsql_connection_checkout_overdue_seconds` histogram.

### Requirement 6: DSQL Reservoir Depth Metrics

**User Story:** As an operator, I want real-time visibility into the reservoir's ready connection count and in-flight connections, so that I can detect capacity pressure before it causes checkout latency.

#### Acceptance Criteria

1. THE Storage_Layer SHALL expose `tokeira_dsql_reservoir_in_flight` as a gauge reflecting the number of connections currently checked out.
2. THE Storage_Layer SHALL expose `tokeira_dsql_reservoir_utilization_ratio` as a gauge reflecting `in_flight / (in_flight + ready)`.
3. THE Storage_Layer SHALL expose `tokeira_dsql_pool_connections_total` as a gauge reflecting the total number of live connections in the reservoir.
4. THE Storage_Layer SHALL expose `tokeira_dsql_pool_empty_reservoir_total` as a counter incremented each time a checkout finds zero ready connections.
5. WHEN the reservoir utilization ratio exceeds 0.8, THE Storage_Layer SHALL emit a structured warning log indicating reservoir pressure.

### Requirement 7: Rate-Limiter Token Metrics

**User Story:** As an operator, I want to see the remaining token count and throttle events in the DSQL connection rate limiter, so that I can detect when connection creation is being throttled.

#### Acceptance Criteria

1. THE Rate_Limiter SHALL expose `tokeira_dsql_rate_limiter_tokens_remaining` as a gauge reflecting the current available tokens.
2. WHEN a connection creation request is throttled, THE Rate_Limiter SHALL increment `tokeira_dsql_rate_limiter_throttled_total`.
3. THE Rate_Limiter SHALL record the wait duration imposed by throttling in `tokeira_dsql_rate_limiter_throttle_duration_seconds` histogram.
4. THE Rate_Limiter SHALL expose `tokeira_dsql_pool_rate_limiter_rate` as a gauge reflecting the configured token replenishment rate.

### Requirement 8: Class-Budget Saturation Metrics

**User Story:** As an operator, I want to see per-class connection budget utilization and waiter counts, so that I can detect when a specific DbClass is starving other classes.

#### Acceptance Criteria

1. THE Storage_Layer SHALL expose `tokeira_dsql_pool_class_budget_total` as a gauge with a `class` label reflecting the configured permit count per DbClass.
2. THE Storage_Layer SHALL expose `tokeira_dsql_pool_class_in_use` as a gauge with a `class` label reflecting the current number of permits held.
3. THE Storage_Layer SHALL expose `tokeira_dsql_pool_class_waiters` as a gauge with a `class` label reflecting the number of tasks waiting for a permit.
4. THE Storage_Layer SHALL record permit acquisition wait time in `tokeira_dsql_class_permit_wait_duration_seconds` histogram with a `class` label.
5. WHEN a class utilization ratio (`in_use / budget_total`) exceeds 0.9 for any class, THE Storage_Layer SHALL emit a structured warning log identifying the saturated class.

### Requirement 9: Distributed Trace Attributes for Full Request Path

**User Story:** As an operator investigating latency, I want each trace span to carry attributes identifying which layer (gRPC, edge, runtime, kernel, storage) it belongs to and the relevant identifiers, so that I can pinpoint where time is spent.

#### Acceptance Criteria

1. WHEN a gRPC request enters the edge layer, THE Edge_Layer SHALL create a root span with attributes: `rpc.method`, `rpc.service`, `tokeira.namespace`, and `tokeira.request_id`.
2. WHEN the edge layer dispatches to the runtime, THE Runtime_Layer SHALL create a child span with attributes: `tokeira.lane_id`, `tokeira.shard_id`, `tokeira.bundle_id`, and `tokeira.command_type`.
3. WHEN the runtime invokes the kernel, THE Runtime_Layer SHALL create a child span with attributes: `tokeira.run_id`, `tokeira.workflow_type`, and `tokeira.transition_number`.
4. WHEN the runtime commits to storage, THE Storage_Layer SHALL create a child span with attributes: `tokeira.storage_operation`, `tokeira.dsql_class`, `tokeira.occ_retries`, and `tokeira.commit_duration_ms`.
5. THE Export_Pipeline SHALL propagate W3C TraceContext headers across all internal async boundaries so that spans form a connected trace.
6. SPANS on high-concurrency storage commit paths and lane execution paths SHALL use explicit `tracing::span!` creation rather than `#[instrument]` to avoid the known `tracing-subscriber` span-clone panic under cancellation.
7. SPANS on low-concurrency paths (gRPC handler entry, controller placement loop, autoscaler control loop) MAY use `#[instrument]` where the concurrency risk is negligible.

### Requirement 10: Dashboard Provisioning

**User Story:** As an operator, I want pre-built Grafana dashboards that cover the key operational views, so that I can monitor tokeirad without building dashboards from scratch.

#### Acceptance Criteria

1. THE Dashboard system SHALL provision a "DSQL Connection Health" dashboard displaying reservoir depth, rate-limiter tokens, class-budget saturation, connection errors, and leak suspects.
2. THE Dashboard system SHALL provision an "OCC Contention" dashboard displaying conflict rates by operation, retry histograms, and commit duration percentiles.
3. THE Dashboard system SHALL update the existing "broker-runtime-health" dashboard to include OCC retry counters and lane processing duration by command type.
4. THE Dashboard system SHALL update the existing "storage-projection-health" dashboard to include migration events and DSQL statement-level duration breakdowns.
5. WHEN a new dashboard JSON file is added to the `platforms/compose/dashboards/` directory, THE Compose_Platform SHALL automatically provision it in Grafana on the next `tkr infra apply`.

### Requirement 11: Alerting Rules

**User Story:** As an operator, I want pre-configured alerting rules for critical observability signals, so that I am notified before failures cascade.

#### Acceptance Criteria

1. THE Alert_Rule system SHALL define a `DsqlReservoirExhaustion` alert that fires when `tokeira_dsql_reservoir_utilization_ratio` exceeds 0.9 for 2 minutes.
2. THE Alert_Rule system SHALL define a `DsqlOccConflictSpike` alert that fires when the rate of `tokeira_storage_dsql_occ_conflict_total` exceeds 50 per second for 1 minute.
3. THE Alert_Rule system SHALL define a `DsqlConnectionLeakDetected` alert that fires when `tokeira_dsql_connection_leak_suspects` is greater than 0 for 5 minutes.
4. THE Alert_Rule system SHALL define a `DsqlRateLimiterThrottling` alert that fires when the rate of `tokeira_dsql_rate_limiter_throttled_total` exceeds 10 per second for 2 minutes.
5. THE Alert_Rule system SHALL define a `DsqlClassBudgetSaturation` alert that fires when any class has `tokeira_dsql_pool_class_in_use / tokeira_dsql_pool_class_budget_total` exceeding 0.95 for 3 minutes.
6. THE Alert_Rule system SHALL be provisioned as a Mimir ruler rules file in the compose platform and as a Prometheus rules file for non-compose deployments.

### Requirement 12: Metric Naming Validation

**User Story:** As a developer, I want all metric names to be validated against the project's naming convention at compile time or test time, so that typos and inconsistencies are caught before deployment.

#### Acceptance Criteria

1. THE Metric_Manifest in each crate SHALL declare all metric names in a `METRIC_NAMES` constant with their `MetricType`.
2. WHEN unit tests run, THE Metric_Manifest validation test SHALL verify that every declared metric name passes `validate_metric_name()` from `tokeira-types`.
3. THE Metric_Manifest SHALL reject metric names that do not start with `tokeira_` prefix.
4. THE Metric_Manifest SHALL reject duration histogram names that do not end with `_seconds`.
5. THE Metric_Manifest SHALL reject counter names that do not end with `_total`.

### Requirement 13: Placement Controller Observability

**User Story:** As an operator, I want visibility into the placement controller's decision-making, routing snapshot health, and drain coordination state, so that I can diagnose shard ownership issues and placement instability.

#### Acceptance Criteria

1. THE Placement_Controller SHALL expose `tokeira_controller_placement_loop_duration_seconds` histogram recording the duration of each placement computation cycle.
2. THE Placement_Controller SHALL expose `tokeira_controller_generation_cas_total` counter with `outcome` label (`success`, `conflict`, `error`) for each generation CAS attempt.
3. THE Placement_Controller SHALL expose `tokeira_controller_routing_snapshot_size` gauge reflecting the number of bundle ownership entries in the current routing snapshot.
4. THE Placement_Controller SHALL expose `tokeira_controller_bundle_ownership_churn_total` counter incremented each time a bundle changes owner.
5. THE Placement_Controller SHALL expose `tokeira_controller_drain_active_nodes` gauge reflecting the number of nodes currently in drain state.
6. THE Placement_Controller SHALL expose `tokeira_controller_budget_allocation_total` counter with `outcome` label for each connection budget allocation CAS attempt.
7. THE Placement_Controller SHALL expose `tokeira_controller_membership_nodes_total` gauge reflecting the number of registered runtime nodes.

### Requirement 14: Autoscaler Observability

**User Story:** As an operator, I want visibility into the autoscaler's scaling decisions, metric freshness, and reconciliation state, so that I can understand why scaling events happen and detect when the autoscaler is operating on stale data.

#### Acceptance Criteria

1. THE Autoscaler SHALL expose `tokeira_autoscaler_loop_duration_seconds` histogram with a `loop` label (`replica`, `scale_out`, `retirement`) recording the duration of each control loop iteration.
2. THE Autoscaler SHALL expose `tokeira_autoscaler_scaling_decisions_total` counter with `loop`, `direction` (`up`, `down`, `hold`), and `reason` labels for each scaling decision.
3. THE Autoscaler SHALL expose `tokeira_autoscaler_metric_freshness_age_seconds` gauge reflecting the age of the most recent metric sample used for scaling decisions.
4. WHEN metric freshness exceeds the configured staleness threshold, THE Autoscaler SHALL increment `tokeira_autoscaler_stale_metrics_total` and suppress scale-in decisions.
5. THE Autoscaler SHALL expose `tokeira_autoscaler_desired_replicas` gauge with a `service` label reflecting the current desired replica count per service.
6. THE Autoscaler SHALL expose `tokeira_autoscaler_nomination_total` counter with `outcome` label (`accepted`, `rejected`, `timeout`) for scale-in candidate nominations.
7. THE Autoscaler SHALL expose `tokeira_autoscaler_leader_lease_held` gauge (1 if this instance holds the leader lease, 0 otherwise).
8. THE Autoscaler SHALL expose `tokeira_autoscaler_mimir_query_duration_seconds` histogram recording the latency of each Mimir metric query.

### Requirement 15: Controller Dashboard

**User Story:** As an operator, I want a pre-built Grafana dashboard for the placement controller, so that I can monitor placement health, ownership churn, and drain progress at a glance.

#### Acceptance Criteria

1. THE Dashboard system SHALL provision a "Placement Controller" dashboard displaying: membership node count, routing snapshot size, bundle ownership churn rate, generation CAS success/failure rates, placement loop duration percentiles, drain active nodes, and budget allocation outcomes.
2. THE Dashboard SHALL organize panels into rows: "Membership", "Placement Decisions", "Drain Coordination", and "Connection Budget".
3. THE Dashboard SHALL follow the project dashboard styling conventions (see Requirement 17).

### Requirement 16: Autoscaler Dashboard

**User Story:** As an operator, I want a pre-built Grafana dashboard for the autoscaler, so that I can monitor scaling decisions, metric freshness, and leader election state.

#### Acceptance Criteria

1. THE Dashboard system SHALL provision an "Autoscaler" dashboard displaying: loop durations by type, scaling decisions by direction/reason, desired vs actual replica counts, metric freshness age, stale metric events, nomination outcomes, leader lease state, and Mimir query latency.
2. THE Dashboard SHALL organize panels into rows: "Control Loops", "Scaling Decisions", "Metric Health", and "Leader Election".
3. THE Dashboard SHALL follow the project dashboard styling conventions (see Requirement 17).

### Requirement 17: Dashboard Styling Conventions

**User Story:** As an operator, I want all dashboards to follow a consistent visual style, so that I can read any dashboard without relearning conventions.

#### Acceptance Criteria

1. EVERY time-series panel SHALL use smooth line interpolation (`lineInterpolation: smooth`) without point markers (`showPoints: never`).
2. EVERY panel SHALL include a description annotation explaining what the panel shows and how to interpret it for operational decisions.
3. EVERY panel with a legend SHALL display meaningful series names (not raw metric names) using legend display mode `table` or `list` as appropriate for the panel density.
4. EVERY panel SHALL declare explicit unit measures in the field configuration (`unit` field) matching the metric's semantic unit (e.g., `s` for seconds, `short` for counts, `percentunit` for ratios).
5. EVERY dashboard SHALL use consistent row-based organization with collapsed rows for secondary detail panels.
6. EVERY dashboard SHALL include a `$datasource` template variable defaulting to the Mimir datasource, allowing operators to switch between data sources.

### Requirement 18: Projection Worker Observability

**User Story:** As an operator, I want visibility into projection worker throughput, lag, and failure rates, so that I can detect when visibility freshness degrades and identify bottlenecks in the projection pipeline.

#### Acceptance Criteria

1. THE Projection_Worker SHALL expose `tokeira_projection_records_processed_total` counter with `partition_id` and `outcome` labels.
2. THE Projection_Worker SHALL expose `tokeira_projection_lag_records` gauge with `partition_id` label reflecting the number of unprocessed records between the worker's cursor and the latest committed transition.
3. THE Projection_Worker SHALL expose `tokeira_projection_batch_apply_duration_seconds` histogram with `partition_id` label recording the time to apply each batch to the visibility sink.
4. THE Projection_Worker SHALL expose `tokeira_projection_sink_errors_total` counter with `partition_id` and `error_kind` labels.
5. THE Projection_Worker SHALL expose `tokeira_projection_checkpoint_lag_seconds` gauge reflecting the time since the last successful checkpoint write.

### Requirement 19: Infrastructure Service Health Metrics

**User Story:** As an operator, I want health metrics for the observability infrastructure services (Alloy, Mimir, Loki, Grafana) themselves, so that I can detect when the monitoring pipeline is degraded before it causes blind spots.

#### Acceptance Criteria

1. THE Alloy sidecar SHALL expose its built-in `/metrics` endpoint and THE Alloy scrape config SHALL include a self-scrape target for Alloy's own metrics (scrape success rate, target count, WAL size).
2. THE Mimir service SHALL expose its built-in `/metrics` endpoint and THE Alloy config SHALL scrape Mimir for ingestion rate, query latency, and compaction health.
3. THE Loki service SHALL expose its built-in `/metrics` endpoint and THE Alloy config SHALL scrape Loki for ingestion rate, query latency, and chunk store health.
4. THE Dashboard system SHALL provision an "Infrastructure Health" dashboard displaying: Alloy scrape success rates per target, Mimir ingestion rate and query p95, Loki ingestion rate and query p95, and Grafana datasource health.
5. THE Alert_Rule system SHALL define a `ScrapeFailing` alert that fires when any Alloy scrape target has a success rate below 0.9 for 5 minutes.

### Requirement 20: Projection Worker Dashboard

**User Story:** As an operator, I want a pre-built Grafana dashboard for projection workers, so that I can monitor visibility freshness, throughput, and error rates.

#### Acceptance Criteria

1. THE Dashboard system SHALL provision a "Projection Workers" dashboard displaying: records processed rate by partition, projection lag by partition, batch apply duration percentiles, sink error rates, and checkpoint lag.
2. THE Dashboard SHALL organize panels into rows: "Throughput", "Freshness & Lag", and "Errors".
3. THE Dashboard SHALL follow the project dashboard styling conventions (see Requirement 17).
