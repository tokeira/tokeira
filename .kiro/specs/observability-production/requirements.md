# Requirements Document

## Introduction

Production-facing observability for the full Tokeira ECS deployment surface: `tokeirad` (edge, runtime, kernel, storage, and embedded projection workers), `tokeira-controller` (placement and routing), `tokeira-autoscaler` (scaling and retirement), projection workers (whether embedded in `tokeirad` or running as a future standalone process), and the supporting observability infrastructure services (Alloy, Mimir, Loki, Grafana, and optional trace backends).

This spec defines the production observability contract for Tokeira. It covers per-process telemetry endpoints, Prometheus scrape, optional OTLP push, structured logging, trace propagation and export, metric naming and cardinality governance, DSQL-specific operational metrics, placement controller metrics, autoscaler metrics, projection worker metrics, infrastructure health signals, health/readiness endpoints, dashboard provisioning, alerting rules, runbooks, and smoke tests.

This spec builds on two completed specs:

- `observability-foundation` — established the `metrics` crate recording API, Prometheus endpoint on `:9090`, `METRIC_NAMES` manifests, and `validate_metric_name()` convention.
- `dsql-observability-metrics` — defined DSQL-specific instrumentation for operation latency, OCC conflicts, reservoir depth, rate limiting, class budgets, projection, per-shard distribution, error classification, and query decomposition.

The `dsql-observability-metrics` spec remains the authoritative source for DSQL metric names, label conventions, and dashboard styling where it is more specific than this document. This spec extends that work across the full production deployment footprint.

### Instrumentation Practice

The following guidance applies to all new span and metric instrumentation in this spec:

- Prefer explicit `tracing::span!` creation around stable boundaries over `#[instrument]` on hot or cancellable async functions.
- Reserve `#[instrument]` for low-concurrency entry points such as gRPC handler entry, controller placement loop entry, autoscaler control-loop entry, and command-line entry points.
- On high-concurrency storage commit paths, reservoir checkout paths, runtime lane paths, and cancellable async loops, use manual span creation to avoid span lifecycle issues under task cancellation.
- Metric recording (`counter!`, `gauge!`, `histogram!`) is safe regardless of concurrency; the constraint applies only to span creation and propagation.
- Metrics and log records SHALL avoid unbounded high-cardinality identifiers as labels. High-cardinality values MAY be included as structured log fields or span attributes where operationally useful.
- Production logs SHALL be structured and machine-readable.
- Trace IDs SHALL be propagated into structured logs where a current span exists.

## Glossary

- **Tokeirad**: The Tokeira server binary hosting gRPC edge, runtime, kernel invocation, and storage access.
- **Edge_Layer**: The gRPC/API ingress layer that accepts client and worker requests.
- **Runtime_Layer**: The execution layer that owns lanes, task dispatch, activity progression, timers, and kernel invocation.
- **Kernel**: The deterministic transition evaluator for workflow state changes.
- **Storage_Layer**: The DSQL-backed persistence layer for histories, hot state, leases, visibility logs, and supporting tables.
- **Placement_Controller**: The `tokeira-controller` binary that manages bundle placement, shard ownership, routing snapshots, connection budget allocation, and drain coordination.
- **Autoscaler**: The `tokeira-autoscaler` binary that manages replica scaling, runtime scale-out, runtime retirement, and scale-in nomination.
- **Projection_Worker**: A projection worker that reads committed transition records and applies visibility updates. Projection currently runs embedded in `tokeirad`; a standalone projection process is a future packaging concern.
- **Export_Pipeline**: The subsystem responsible for exposing, pushing, and forwarding telemetry.
- **Prometheus_Endpoint**: The HTTP `/metrics` endpoint that exposes metrics in Prometheus exposition format.
- **OTLP_Exporter**: The OpenTelemetry Protocol exporter that pushes metrics or traces to a configured collector using gRPC or HTTP.
- **Structured_Log**: A JSON log record with stable fields suitable for Loki ingestion and incident correlation.
- **Trace_Context**: The propagated request context, including W3C TraceContext identifiers, that links a request across asynchronous boundaries.
- **OCC_Conflict**: An optimistic concurrency control conflict raised by Aurora DSQL, including SQLSTATE `40001` serialization failures.
- **Retry_Histogram**: A histogram metric recording retry attempt counts or retry-related wait durations.
- **Migration_Event**: A structured log and metric emitted when a DSQL schema migration is applied, skipped, rolled back, or fails.
- **Connection_Leak_Detector**: A subsystem that identifies DSQL connections checked out from the reservoir but not returned within a configurable deadline.
- **Reservoir**: The channel-based connection buffer that pre-creates DSQL connections so checkout does not perform connection establishment on the request hot path.
- **Rate_Limiter**: The token-bucket mechanism that enforces the DSQL cluster-wide connection creation rate.
- **Class_Budget**: The per-DbClass connection permit allocation, such as Control, Commit, Read, Projection, and Maintenance.
- **Metric_Manifest**: A per-crate declaration of metric names, metric types, allowed labels, and label cardinality expectations.
- **Dashboard**: A Grafana JSON model provisioned automatically by the platform.
- **Alert_Rule**: A Prometheus or Mimir ruler rule that fires when a metric expression crosses a configured threshold for a sustained duration.
- **Runbook**: Operator documentation linked from an alert that describes likely causes, diagnostic queries, dashboard links, and safe remediation steps.
- **Alloy**: The Grafana telemetry collector used to scrape metrics, collect logs, and forward telemetry.
- **Mimir**: The Grafana-compatible long-term metrics store used in compose and ECS observability stacks.
- **Loki**: The log store used by the Tokeira observability stack.
- **Tempo**: The optional trace backend used when production trace storage is enabled.

## Requirements

### Requirement 1: Per-Process Telemetry Surfaces

**User Story:** As an operator, I want every production Tokeira process to expose its own telemetry surface, so that I can monitor each service independently and detect missing targets.

#### Acceptance Criteria

1. THE `tokeirad`, `tokeira-controller`, `tokeira-autoscaler`, and projection workers SHALL each expose a Prometheus-compatible `/metrics` endpoint from the process that hosts them.
2. THE `tokeirad`, `tokeira-controller`, `tokeira-autoscaler`, and projection workers SHALL each expose structured logs suitable for Alloy collection and Loki ingestion from the process that hosts them.
3. THE `tokeirad`, `tokeira-controller`, `tokeira-autoscaler`, and projection workers SHALL each support trace context propagation and trace export when tracing is enabled.
4. THE Alloy scrape configuration SHALL include one scrape job per process type with stable labels: `service`, `cluster`, `deployment`, `task_id`, and `node_id` where available.
5. WHEN a process is unavailable, THE scrape target SHALL become visibly unhealthy through scrape metrics rather than being hidden by aggregation.
6. THE Dashboard system SHALL include scrape health panels per process type.

### Requirement 2: Prometheus Scrape Endpoint Completeness

**User Story:** As an operator, I want each Prometheus scrape endpoint to expose all metrics registered by the process, so that no process-local operational signal is missing.

#### Acceptance Criteria

1. THE Prometheus_Endpoint SHALL expose all metrics declared in the process Metric_Manifest and recorded by the shared metrics facade.
2. WHEN a metric is recorded by a process, THE Prometheus_Endpoint for that process SHALL include that metric in the next scrape response within one scrape interval.
3. THE Prometheus_Endpoint SHALL include a `tokeira_build_info` gauge with `version`, `commit`, `rustc_version`, and `service` labels.
4. THE Prometheus_Endpoint SHALL include a `tokeira_process_metadata_start_time_seconds` gauge indicating the Unix timestamp at which the process started.
5. THE Prometheus_Endpoint SHALL respond to GET requests on `/metrics` with HTTP status 200 and Prometheus exposition content type.
6. IF a process has no samples for a declared metric, THEN THE Prometheus_Endpoint MAY omit that metric until it is first recorded unless the metric is a required build or process metadata metric.

### Requirement 3: OTLP Metrics Push Export

**User Story:** As an operator deploying to environments without a Prometheus scraper, I want Tokeira processes to push metrics via OTLP, so that I can collect metrics in push-based architectures.

**Phase:** 2 (deferred from Phase 1 implementation).

**Phase Note:** OTLP metrics push is a Phase 2 capability. Phase 1 delivers Prometheus scrape completeness. The OTLP exporter requires a fanout recorder or metrics-bridge design that is deferred until the Prometheus surface is validated.

#### Acceptance Criteria

1. WHEN `infrastructure.observability.otlp_metrics_enabled` is true AND the OTLP metrics bridge is implemented in Phase 2, THE OTLP_Exporter SHALL push metric data to the configured `otlp_metrics_endpoint` using the configured protocol (`grpc` or `http`).
2. THE OTLP_Exporter SHALL export all metrics declared in the process Metric_Manifest and recorded by the shared metrics facade.
3. IF the OTLP endpoint is unreachable, THEN THE OTLP_Exporter SHALL buffer metric batches in memory up to a configurable limit and retry with exponential backoff.
4. IF the buffer limit is exceeded, THEN THE OTLP_Exporter SHALL drop the oldest batch and increment `tokeira_otlp_metrics_dropped_batches_total` with a `service` label.
5. WHILE OTLP metrics export is enabled, THE OTLP_Exporter SHALL include resource attributes identifying cluster name, deployment name, service name, node identity, task identity, version, and commit.
6. WHEN the process receives a graceful shutdown signal, THE OTLP_Exporter SHALL flush pending metric batches before exit, bounded by a configurable timeout.
7. IF the graceful shutdown flush times out, THEN THE OTLP_Exporter SHALL increment `tokeira_otlp_metrics_flush_timeout_total` before process exit when possible.

### Requirement 4: Structured Log Pipeline

**User Story:** As an operator, I want Tokeira services to emit structured logs with consistent fields, so that I can correlate logs with metrics, traces, runtime nodes, and workflow executions.

#### Acceptance Criteria

1. EVERY Tokeira process SHALL emit JSON structured logs in production mode.
2. EVERY Structured_Log SHALL include `timestamp`, `level`, `target`, `service`, `cluster`, `deployment`, and `message` fields.
3. EVERY Structured_Log emitted by an ECS task SHALL include `task_id` where available.
4. EVERY Structured_Log emitted by a runtime node SHALL include `node_id` where available.
5. WHEN a log is emitted inside a trace span, THE Structured_Log SHALL include `trace_id` and `span_id` fields.
6. WHEN safe and applicable, workflow-related Structured_Log records SHALL include `namespace`, `workflow_type`, `run_id`, `shard_id`, and `bundle_id` as fields.
7. THE Log_Pipeline SHALL NOT promote `workflow_id`, `run_id`, `request_id`, `trace_id`, raw SQL text, raw error messages, node endpoint, or task ARN to Loki labels.
8. THE Alloy configuration SHALL collect logs from all Tokeira ECS tasks and forward them to Loki.
9. THE Log_Pipeline SHALL preserve ERROR and WARN logs without sampling.
10. IF log forwarding fails, THE Alloy or process-level telemetry SHALL expose failure counters or health signals indicating degraded log delivery.

### Requirement 5: Distributed Trace Propagation and Span Attributes

**User Story:** As an operator investigating latency, I want correlated traces across gRPC ingress, edge routing, runtime dispatch, kernel evaluation, and DSQL commit, so that I can pinpoint where time is spent.

#### Acceptance Criteria

1. WHEN a gRPC request enters the edge layer, THE Edge_Layer SHALL create or join a root request span with attributes: `rpc.system`, `rpc.method`, `rpc.service`, `server.address`, `tokeira.namespace`, and `tokeira.request_id`.
2. WHEN the edge layer dispatches to the runtime, THE Runtime_Layer SHALL create a child span with attributes: `tokeira.lane_id`, `tokeira.shard_id`, `tokeira.bundle_id`, and `tokeira.command_type` where known.
3. WHEN the runtime invokes the Kernel, THE Runtime_Layer SHALL create a child span with attributes: `tokeira.run_id`, `tokeira.workflow_type`, and `tokeira.transition_number` where known.
4. WHEN the runtime commits to storage, THE Storage_Layer SHALL create a child span with attributes: `tokeira.storage_operation`, `tokeira.dsql_class`, and `tokeira.occ_retries` where known.
5. THE Export_Pipeline SHALL propagate W3C TraceContext headers across internal async boundaries so that spans are correlated via shared trace context. Where channel-based dispatch prevents parent-child continuity, correlation is achieved via `origin_trace_id` and `origin_span_id` attributes on the receiving span.
6. SPANS on high-concurrency storage commit paths, reservoir checkout paths, and lane execution paths SHALL use explicit `tracing::span!` creation rather than `#[instrument]`.
7. SPANS on low-concurrency paths, including gRPC handler entry, controller placement loop entry, autoscaler control loop entry, and CLI entry points, MAY use `#[instrument]`.
8. THE Trace_Context SHALL be propagated through internal gRPC requests, membership streams, and routing snapshot streams via W3C TraceContext headers. For runtime dispatch channels, trace correlation SHALL use `origin_trace_id` and `origin_span_id` attributes rather than parent-child span linking.
9. THE Trace_Context SHALL NOT require workflow history to store trace IDs for correctness.

### Requirement 6: Distributed Trace Export and Sampling

**User Story:** As an operator, I want production traces to be exported and sampled safely, so that latency investigations are possible without overwhelming the observability stack.

#### Acceptance Criteria

1. WHEN `infrastructure.observability.tracing_enabled` is true, THE Trace_Exporter SHALL export spans via OTLP to the configured trace backend.
2. THE Trace_Exporter SHALL support configurable head sampling.
3. THE Trace_Exporter SHALL support error-biased sampling for operationally significant failures.
4. THE Trace_Exporter SHALL always sample traces containing storage commit errors, OCC retry exhaustion, `NotShardOwner` errors, projection sink failures, migration failures, controller placement errors, and autoscaler reconciliation errors.
5. THE Trace_Exporter SHALL include resource attributes identifying cluster name, deployment name, service name, node identity, task identity, version, and commit.
6. THE Log_Pipeline SHALL include `trace_id` and `span_id` fields in structured logs whenever a current span exists.
7. THE Metrics_Pipeline SHOULD attach trace exemplars to latency histograms when the configured metrics exporter and backend support exemplars.
8. IF no trace backend is configured, THEN traces SHALL be disabled by default while trace IDs MAY still be included in logs generated from inbound trace contexts.

### Requirement 7: Metric Naming Validation

**User Story:** As a developer, I want all metric names to be validated against the project's naming convention at compile time or test time, so that typos and inconsistencies are caught before deployment.

#### Acceptance Criteria

1. THE Metric_Manifest in each crate SHALL declare all metric names in a `METRIC_NAMES` constant with their `MetricType`.
2. WHEN unit tests run, THE Metric_Manifest validation test SHALL verify that every declared metric name passes `validate_metric_name()` from `tokeira-types`.
3. THE Metric_Manifest SHALL reject metric names that do not start with the `tokeira_` prefix.
4. THE Metric_Manifest SHALL reject duration histogram names that do not end with `_seconds`.
5. THE Metric_Manifest SHALL reject counter names that do not end with `_total`.
6. THE Metric_Manifest SHALL reject ratio gauge names that do not end with `_ratio` unless explicitly exempted.
7. THE Metric_Manifest SHALL reject metadata gauge names that do not end with `_info` unless explicitly exempted by name.
8. THE Metric_Manifest validation test SHALL verify that the declared metric type matches the expected suffix convention.

### Requirement 8: Metric Label Cardinality Governance

**User Story:** As an operator, I want metric labels to be bounded and reviewed, so that observability does not create unbounded Mimir cardinality or incident-time blind spots.

#### Acceptance Criteria

1. THE Metric_Manifest SHALL declare the allowed label keys for every metric.
2. THE Metric_Manifest SHALL declare whether each label has a bounded enum, bounded numeric range, configuration-bounded range, or unbounded value source.
3. THE validation test SHALL fail if a metric includes unbounded labels such as `workflow_id`, `run_id`, `request_id`, `trace_id`, raw SQL text, raw error message, node endpoint, task ARN, or arbitrary user input.
4. THE labels `operation`, `outcome`, `class`, `loop`, `direction`, `reason`, `error_kind`, `service`, and `status` SHALL be backed by enums or constrained constant sets.
5. THE `partition_id`, `shard_id`, and `bundle_id` labels SHALL only be used where the maximum cardinality is bounded by configuration and documented in the Metric_Manifest.
6. THE metrics crate SHALL provide typed recording helpers for metrics whose labels are considered high-risk.
7. WHEN label validation tests run, THE tests SHALL verify that all declared label names use snake_case.
8. THE Dashboard system SHALL prefer template variables or top-N panels over displaying all series for metrics with configuration-bounded high cardinality.
9. THE Metric_Manifest SHALL classify existing labels into one of: enum-bounded, configuration-bounded with documented maximum, unbounded-but-accepted with explicit justification, or to-be-migrated.
10. Labels classified as unbounded-but-accepted SHALL include a cardinality budget estimate and a comment explaining why they are acceptable.

### Requirement 9: DSQL OCC Conflict and Retry Metrics

**User Story:** As an operator, I want to see OCC conflict rates and retry distributions per storage operation, so that I can detect contention hotspots and tune shard placement.

#### Acceptance Criteria

1. WHEN an OCC conflict occurs, THE Storage_Layer SHALL increment `tokeira_storage_dsql_occ_conflict_total` with an `operation` label identifying the storage operation.
2. WHEN a storage operation completes after retries, THE Storage_Layer SHALL record the retry attempt count in the existing `tokeira_storage_dsql_commit_retries` histogram.
3. THE Storage_Layer SHALL increment `tokeira_storage_dsql_retry_total` with `operation` and `outcome` labels distinguishing `success`, `exhausted`, and `not_retried` outcomes.
4. WHEN the retry budget is exhausted, THE Storage_Layer SHALL increment `tokeira_storage_dsql_retry_total` with `outcome="exhausted"`.
5. THE Storage_Layer SHALL record operation duration in the existing `tokeira_storage_dsql_operation_duration_seconds` histogram with `operation` and `outcome` labels, and detailed statement duration in `tokeira_storage_dsql_statement_duration_seconds` where available.
6. THE Storage_Layer SHALL classify retryable and non-retryable storage errors using bounded `error_kind` values.

### Requirement 10: Migration Event Observability

**User Story:** As an operator, I want to know when schema migrations run, succeed, skip, roll back, or fail, so that I can correlate deployment events with behavioural changes.

#### Acceptance Criteria

1. WHEN a DSQL schema migration is applied successfully, THE Migration_Event system SHALL emit a Structured_Log at INFO level containing migration filename, duration, previous schema version, and resulting schema version.
2. WHEN a DSQL schema migration is skipped because it has already been applied, THE Migration_Event system SHALL emit a Structured_Log at DEBUG or INFO level containing migration filename and current schema version.
3. WHEN a DSQL schema migration fails, THE Migration_Event system SHALL emit a Structured_Log at ERROR level containing migration filename, error kind, sanitized error message, and SQLSTATE code when available.
4. THE Migration_Event system SHALL increment `tokeira_storage_migration_applied_total` with a `status` label (`success`, `failure`, `skipped`, or `rolled_back`) for each migration attempt.
5. THE Migration_Event system SHALL record migration duration in `tokeira_storage_migration_duration_seconds` histogram.
6. THE Migration_Event system SHALL include `trace_id` and `span_id` in migration logs when migrations run inside a trace context.

### Requirement 11: DSQL Connection Leak Detection

**User Story:** As an operator, I want to detect DSQL connections that are checked out but not returned, so that I can identify code paths that leak connections and prevent reservoir exhaustion.

#### Acceptance Criteria

1. WHEN a connection has been checked out from the Reservoir for longer than a configurable deadline, defaulting to 60 seconds, THE Connection_Leak_Detector SHALL emit a Structured_Log at WARN level containing checkout duration, DbClass, and checkout call-site identifier.
2. THE checkout call-site identifier SHALL be a low-cardinality explicit instrumentation value rather than a full captured stack trace on the hot path.
3. THE Connection_Leak_Detector SHALL increment `tokeira_dsql_connection_leak_detected_total` with a `class` label each time a leak suspect is detected.
4. THE Connection_Leak_Detector SHALL set `tokeira_dsql_connection_leak_suspects` gauge reflecting the current number of connections exceeding the leak deadline.
5. IF a suspected leaked connection is eventually returned, THEN THE Connection_Leak_Detector SHALL decrement the suspects gauge and record total checkout duration in `tokeira_dsql_connection_checkout_overdue_seconds` histogram.
6. THE Connection_Leak_Detector SHALL NOT include connection credentials, connection strings, or SQL text in logs.

### Requirement 12: DSQL Reservoir Depth and Refill Metrics

**User Story:** As an operator, I want real-time visibility into the reservoir's ready connection count, in-flight connections, target size, and refill health, so that I can detect capacity pressure before it causes checkout latency.

#### Acceptance Criteria

1. THE Storage_Layer SHALL expose `tokeira_dsql_reservoir_ready_connections` as a gauge reflecting the number of ready connections available for checkout.
2. THE Storage_Layer SHALL expose `tokeira_dsql_reservoir_in_flight` as a gauge reflecting the number of connections currently checked out.
3. THE Storage_Layer SHALL expose `tokeira_dsql_reservoir_target_connections` as a gauge reflecting the configured target number of live reservoir connections.
4. THE Storage_Layer SHALL expose `tokeira_dsql_reservoir_utilization_ratio` as a gauge reflecting `in_flight / max(in_flight + ready, 1)`.
5. THE Storage_Layer SHALL expose `tokeira_dsql_pool_connections_total` as a gauge reflecting the total number of live connections in the reservoir.
6. THE Storage_Layer SHALL expose `tokeira_dsql_pool_empty_reservoir_total` as a counter incremented each time a checkout finds zero ready connections.
7. THE Storage_Layer SHALL expose `tokeira_dsql_reservoir_refill_errors_total` as a counter with `error_kind` label.
8. THE Storage_Layer SHALL record reservoir refill duration in `tokeira_dsql_reservoir_refill_duration_seconds` histogram.
9. WHEN the reservoir utilization ratio exceeds the configured warning threshold, defaulting to 0.8, THE Storage_Layer SHALL emit a Structured_Log at WARN level indicating reservoir pressure.

### Requirement 13: DSQL Rate-Limiter Token Metrics

**User Story:** As an operator, I want to see the remaining token count and throttle events in the DSQL connection rate limiter, so that I can detect when connection creation is being throttled.

#### Acceptance Criteria

1. THE Rate_Limiter SHALL expose `tokeira_dsql_rate_limiter_tokens_remaining` as a gauge reflecting the current available tokens.
2. WHEN a connection creation request is throttled, THE Rate_Limiter SHALL increment `tokeira_dsql_rate_limiter_throttled_total`.
3. THE Rate_Limiter SHALL record the wait duration imposed by throttling in `tokeira_dsql_rate_limiter_throttle_duration_seconds` histogram.
4. THE Rate_Limiter SHALL expose `tokeira_dsql_pool_rate_limiter_rate` as a gauge reflecting the configured token replenishment rate.
5. THE Rate_Limiter SHALL expose `tokeira_dsql_pool_rate_limiter_burst` as a gauge reflecting the configured burst capacity.
6. WHEN throttling exceeds the configured warning threshold, THE Rate_Limiter SHALL emit a Structured_Log at WARN level with bounded reason values.

### Requirement 14: DSQL Class-Budget Saturation Metrics

**User Story:** As an operator, I want to see per-class connection budget utilization and waiter counts, so that I can detect when a specific DbClass is starving other classes.

#### Acceptance Criteria

1. THE Storage_Layer SHALL expose `tokeira_dsql_pool_class_budget_total` as a gauge with a `class` label reflecting the configured permit count per DbClass.
2. THE Storage_Layer SHALL expose `tokeira_dsql_pool_class_in_use` as a gauge with a `class` label reflecting the current number of permits held.
3. THE Storage_Layer SHALL expose `tokeira_dsql_pool_class_waiters` as a gauge with a `class` label reflecting the number of tasks waiting for a permit.
4. THE Storage_Layer SHALL record permit acquisition wait time in `tokeira_dsql_class_permit_wait_duration_seconds` histogram with a `class` label.
5. WHEN a class-budget observation finds a permit acquisition that has remained pending for at least 5 seconds and that class has not emitted a pressure warning in the preceding 60 seconds, THE Storage_Layer SHALL emit a Structured_Log at WARN level identifying the class and the longest current permit wait.
6. THE Storage_Layer SHALL expose `tokeira_dsql_pool_class_saturation_ratio` as a gauge with a `class` label.
7. WHILE no permit acquisition has remained pending for at least 5 seconds, THE Storage_Layer SHALL NOT emit a class-budget pressure warning, including when a single-permit class is fully occupied.
8. THE Storage_Layer SHALL observe pending class permit acquisitions at least on each existing 5-second class-budget reporting tick, including acquisitions that never complete.
9. WHEN a permit acquisition succeeds, fails, or is cancelled, THE Storage_Layer SHALL remove that acquisition from current pressure accounting.
10. THE Storage_Layer SHALL preserve the per-class 60-second warning cooldown across recovery and budget reconfiguration within one connection director's lifetime.
11. THE Storage_Layer SHALL continue recording class-budget utilization independently of warning eligibility and SHALL preserve the embedded connection defaults.

### Requirement 15: Runtime and Lane Observability

**User Story:** As an operator, I want visibility into runtime lane throughput, queueing, task execution, and ownership errors, so that I can diagnose workflow execution bottlenecks.

#### Acceptance Criteria

1. THE Runtime_Layer SHALL expose `tokeira_runtime_lane_processing_duration_seconds` histogram with bounded `command_type` label.
2. THE Runtime_Layer SHALL expose `tokeira_runtime_lane_queue_depth` gauge with a `lane_id` label bounded by configured `lane_count` (typically 4-16, but deployment-configurable).
3. THE Runtime_Layer SHALL expose `tokeira_runtime_workflow_task_started_total` counter with `outcome` label.
4. THE Runtime_Layer SHALL expose `tokeira_runtime_workflow_task_completed_total` counter with `outcome` label.
5. THE Runtime_Layer SHALL expose `tokeira_runtime_activity_task_completed_total` counter with `outcome` label.
6. THE Runtime_Layer SHALL expose `tokeira_runtime_not_shard_owner_total` counter with `operation` label.
7. THE Runtime_Layer SHALL emit a Structured_Log at WARN level when repeated `NotShardOwner` errors indicate stale routing state.
8. THE Runtime_Layer SHALL avoid unbounded workflow identifiers as metric labels.

### Requirement 16: Edge and Broker Observability

**User Story:** As an operator, I want visibility into gRPC ingress, worker polling, broker dispatch, and admission pressure, so that worker poll storms cannot hide or starve normal API traffic.

#### Acceptance Criteria

1. THE Edge_Layer SHALL expose the existing `tokeira_edge_grpc_request_total` counter with bounded `method`, `namespace`, and `status` labels.
2. THE Edge_Layer SHALL expose the existing `tokeira_edge_grpc_request_duration_seconds` histogram with bounded `method` and `namespace` labels.
3. THE Edge_Layer SHALL expose `tokeira_edge_worker_poll_requests_total` counter with bounded `task_queue_kind` and `outcome` labels.
4. THE Edge_Layer SHALL expose `tokeira_edge_poll_admission_rejected_total` counter with bounded `reason` label.
5. THE Edge_Layer SHALL expose `tokeira_edge_broker_dispatch_duration_seconds` histogram with bounded `task_type` label.
6. THE Edge_Layer SHALL expose `tokeira_edge_broker_queue_depth` gauge with bounded `task_type` label.
7. WHEN poll admission rejects requests due to saturation, THE Edge_Layer SHALL emit a Structured_Log at WARN level with bounded reason values.

### Requirement 17: Placement Controller Observability

**User Story:** As an operator, I want visibility into the placement controller's decision-making, routing snapshot health, and drain coordination state, so that I can diagnose shard ownership issues and placement instability.

#### Acceptance Criteria

1. THE Placement_Controller SHALL expose `tokeira_controller_placement_loop_duration_seconds` histogram recording the duration of each placement computation cycle.
2. THE Placement_Controller SHALL expose `tokeira_controller_generation_cas_total` counter with `outcome` label (`success`, `conflict`, `error`) for each generation CAS attempt.
3. THE Placement_Controller SHALL expose `tokeira_controller_routing_snapshot_size` gauge reflecting the number of bundle ownership entries in the current routing snapshot.
4. THE Placement_Controller SHALL expose `tokeira_controller_bundle_ownership_churn_total` counter incremented each time a bundle changes owner.
5. THE Placement_Controller SHALL expose `tokeira_controller_drain_active_nodes` gauge reflecting the number of nodes currently in drain state.
6. THE Placement_Controller SHALL expose `tokeira_controller_budget_allocation_total` counter with `outcome` label for each connection budget allocation CAS attempt.
7. THE Placement_Controller SHALL expose `tokeira_controller_membership_nodes_total` gauge reflecting the number of registered runtime nodes.
8. THE Placement_Controller SHALL expose `tokeira_controller_routing_publish_total` counter with `outcome` label for routing snapshot publish attempts.
9. THE Placement_Controller SHALL emit Structured_Log records for placement decisions that cause ownership churn, including previous owner, next owner, bundle ID, generation, and bounded reason values.

### Requirement 18: Autoscaler Observability

**User Story:** As an operator, I want visibility into the autoscaler's scaling decisions, metric freshness, and reconciliation state, so that I can understand why scaling events happen and detect when the autoscaler is operating on stale data.

#### Acceptance Criteria

1. THE Autoscaler SHALL expose `tokeira_autoscaler_loop_duration_seconds` histogram with a `loop` label (`replica`, `scale_out`, `retirement`) recording the duration of each control loop iteration.
2. THE Autoscaler SHALL expose `tokeira_autoscaler_scaling_decisions_total` counter with `loop`, `direction` (`up`, `down`, `hold`), and bounded `reason` labels for each scaling decision.
3. THE Autoscaler SHALL expose `tokeira_autoscaler_metric_freshness_age_seconds` gauge reflecting the age of the most recent metric sample used for scaling decisions.
4. WHEN metric freshness exceeds the configured staleness threshold, THE Autoscaler SHALL increment `tokeira_autoscaler_stale_metrics_total` and suppress scale-in decisions.
5. THE Autoscaler SHALL expose `tokeira_autoscaler_desired_replicas` gauge with a `service` label reflecting the current desired replica count per service.
6. THE Autoscaler SHALL expose `tokeira_autoscaler_actual_replicas` gauge with a `service` label reflecting the observed actual replica count per service.
7. THE Autoscaler SHALL expose `tokeira_autoscaler_nomination_total` counter with `outcome` label (`accepted`, `rejected`, `timeout`) for scale-in candidate nominations.
8. THE Autoscaler SHALL expose `tokeira_autoscaler_active_reconciler_lease_held` gauge, set to 1 if this instance holds the active reconciler lease and 0 otherwise.
9. THE Autoscaler SHALL expose `tokeira_autoscaler_mimir_query_duration_seconds` histogram recording the latency of each Mimir metric query.
10. THE Autoscaler SHALL emit Structured_Log records for scaling decisions, including loop, direction, reason, previous desired count, next desired count, metric freshness, and suppressed scale-in status.

### Requirement 19: Projection Worker Observability

**User Story:** As an operator, I want visibility into projection worker throughput, lag, cursor progression, and failure rates, so that I can detect when visibility freshness degrades and identify bottlenecks in the projection pipeline.

#### Acceptance Criteria

1. THE Projection_Worker SHALL add a bounded `outcome` label to the existing `tokeira_projection_records_processed_total` counter, which already has a `partition_id` label.
2. THE Projection_Worker SHALL expose the existing projection lag gauge (`tokeira_projection_worker_lag_records`) with `partition_id` label reflecting the number of unprocessed records between the worker cursor and the latest committed transition.
3. THE Projection_Worker SHALL expose the existing sink write duration histogram (`tokeira_projection_sink_write_duration_seconds`) with `partition_id` label recording the time to apply each batch to the visibility sink.
4. THE Projection_Worker SHALL expose `tokeira_projection_worker_batch_records` histogram with `partition_id` label recording the number of records processed per batch.
5. THE Projection_Worker SHALL add a bounded `error_kind` label to the existing projection sink error counter (`tokeira_projection_sink_error_total`), which already has a `partition_id` label.
6. THE Projection_Worker SHALL expose `tokeira_projection_checkpoint_lag_seconds` gauge reflecting the time since the last successful checkpoint write.
7. THE Projection_Worker SHALL expose `tokeira_projection_checkpoint_transition_sequence` gauge with `partition_id` label reflecting the latest persisted checkpoint sequence.
8. THE Projection_Worker SHALL expose `tokeira_projection_latest_transition_sequence` gauge with `partition_id` label reflecting the latest known transition sequence available for projection.
9. THE Projection_Worker SHALL expose `tokeira_projection_poll_empty_total` counter with `partition_id` label for empty projection polling cycles.
10. THE Projection_Worker SHALL emit Structured_Log records for repeated sink failures and checkpoint write failures.

### Requirement 20: Infrastructure Service Health Metrics

**User Story:** As an operator, I want health metrics for the observability infrastructure services themselves, so that I can detect monitoring pipeline degradation before it causes blind spots.

#### Acceptance Criteria

1. THE Alloy service SHALL expose its built-in `/metrics` endpoint and THE Alloy scrape config SHALL include a self-scrape target for Alloy metrics.
2. THE Alloy scrape config SHALL scrape Alloy metrics for scrape success, target count, component health, queue health, and WAL health where available.
3. THE Mimir service SHALL expose its built-in `/metrics` endpoint and THE Alloy config SHALL scrape Mimir for ingestion rate, query latency, compaction health, ruler health, and storage errors.
4. THE Loki service SHALL expose its built-in `/metrics` endpoint and THE Alloy config SHALL scrape Loki for ingestion rate, query latency, distributor health, ingester health, and chunk store health.
5. THE Grafana service SHALL expose health status sufficient for dashboards to report datasource health.
6. THE Dashboard system SHALL provision an "Infrastructure Health" dashboard displaying Alloy scrape success rates per target, Mimir ingestion rate and query p95, Loki ingestion rate and query p95, Grafana datasource health, and alert/ruler health.
7. THE Alert_Rule system SHALL define a `ScrapeFailing` alert that fires when any Alloy scrape target has a success rate below the configured threshold for the configured duration.
8. THE Alert_Rule system SHALL define a `TelemetryIngestionStalled` alert that fires when no fresh samples are ingested for a critical Tokeira service while that service is expected to be running.

### Requirement 21: Process Health and Readiness Endpoints

**User Story:** As an operator, I want Tokeira services to expose liveness and readiness endpoints, so that ECS and deployment tooling can distinguish crashed processes from unavailable dependencies.

#### Acceptance Criteria

1. EVERY Tokeira process SHALL expose `/healthz` for process liveness.
2. EVERY Tokeira process SHALL expose `/readyz` for dependency readiness.
3. THE `/healthz` endpoint SHALL return success if the process event loop is alive and able to respond.
4. THE `/readyz` endpoint for `tokeirad` SHALL fail if storage is unavailable, required routing ownership is absent, or critical runtime loops are not running.
5. THE `/readyz` endpoint for `tokeira-controller` SHALL fail if the controller cannot read or write placement state.
6. THE `/readyz` endpoint for `tokeira-autoscaler` SHALL fail if the autoscaler cannot query Mimir or reach required ECS control-plane APIs.
7. THE `/readyz` endpoint for the process hosting projection workers SHALL fail if those workers cannot read projection input or write checkpoints.
8. THE health and readiness endpoints SHALL NOT require authentication on the task-local health port used by ECS health checks.
9. THE health and readiness endpoints SHALL NOT expose secrets, connection strings, or raw dependency errors in response bodies.

### Requirement 22: Dashboard Provisioning

**User Story:** As an operator, I want pre-built Grafana dashboards that cover the key operational views, so that I can monitor Tokeira without building dashboards from scratch.

#### Acceptance Criteria

1. THE Dashboard system SHALL provision a "Tokeira Overview" dashboard displaying high-level service health, scrape health, request rates, error rates, storage latency, OCC conflicts, runtime throughput, projection freshness, controller health, and autoscaler decisions.
2. THE Dashboard system SHALL provision a "DSQL Connection Health" dashboard displaying reservoir ready connections, in-flight connections, utilization ratio, target connections, rate-limiter tokens, throttle duration, class-budget saturation, connection errors, refill errors, and leak suspects.
3. THE Dashboard system SHALL provision an "OCC Contention" dashboard displaying conflict rates by operation, retry attempt histograms, retry exhaustion, commit duration percentiles, and top contended operation classes.
4. THE Dashboard system SHALL update or provision a "Broker Runtime Health" dashboard displaying poll admission, broker dispatch, lane queue depth, lane processing duration, task completion outcomes, and `NotShardOwner` errors.
5. THE Dashboard system SHALL update or provision a "Storage Projection Health" dashboard displaying migration events, DSQL statement-level duration breakdowns, projection lag, projection throughput, and checkpoint health.
6. THE Dashboard system SHALL provision a "Placement Controller" dashboard displaying membership node count, routing snapshot size, bundle ownership churn rate, generation CAS success/failure rates, placement loop duration percentiles, drain active nodes, routing publish outcomes, and budget allocation outcomes.
7. THE Dashboard system SHALL provision an "Autoscaler" dashboard displaying loop durations by type, scaling decisions by direction/reason, desired vs actual replica counts, metric freshness age, stale metric events, nomination outcomes, active reconciler lease state, and Mimir query latency.
8. THE Dashboard system SHALL provision a "Projection Workers" dashboard displaying records processed rate by partition, projection lag by partition, batch apply duration percentiles, sink error rates, checkpoint lag, and checkpoint sequence progression.
9. THE Dashboard system SHALL provision an "Infrastructure Health" dashboard as described by Requirement 20.
10. WHEN a new dashboard JSON file is added to the platform dashboard directory, THE Compose_Platform SHALL automatically provision it in Grafana on the next `tkr infra apply`.
11. WHEN dashboard provisioning fails, THE platform apply operation SHALL surface the failure clearly and SHALL NOT silently ignore invalid dashboard JSON.

### Requirement 23: Dashboard Styling and Validation

**User Story:** As an operator, I want all dashboards to follow a consistent visual style, so that I can read any dashboard without relearning conventions.

#### Acceptance Criteria

1. EVERY time-series panel SHALL use smooth line interpolation (`lineInterpolation: smooth`) without point markers (`showPoints: never`) unless a panel-specific exception is documented.
2. EVERY panel SHALL include a description annotation explaining what the panel shows and how to interpret it for operational decisions.
3. EVERY panel with a legend SHALL display meaningful series names rather than raw metric names.
4. EVERY panel with a legend SHALL use legend display mode `table` or `list` as appropriate for the panel density.
5. EVERY panel SHALL declare explicit unit measures in the field configuration matching the metric's semantic unit, such as `s` for seconds, `short` for counts, `ops` for rates, and `percentunit` for ratios.
6. EVERY dashboard SHALL use consistent row-based organization with collapsed rows for secondary detail panels.
7. EVERY dashboard SHALL include a `$datasource` template variable defaulting to the Mimir datasource, allowing operators to switch between data sources.
8. THE Dashboard_Validator SHALL parse every dashboard JSON file and verify that panels follow the project styling conventions.
9. THE Dashboard_Validator SHALL fail tests for dashboards that reference unknown metric names unless the metric is explicitly marked as external infrastructure telemetry.
10. THE Dashboard_Validator SHALL verify that dashboard UIDs are stable and suitable for runbook links.

### Requirement 24: Alerting Rules

**User Story:** As an operator, I want pre-configured alerting rules for critical observability signals, so that I am notified before failures cascade.

#### Acceptance Criteria

1. THE Alert_Rule system SHALL define a `DsqlReservoirExhaustion` alert that fires when `tokeira_dsql_reservoir_utilization_ratio` exceeds the configured threshold, defaulting to 0.9, for the configured duration, defaulting to 2 minutes.
2. THE Alert_Rule system SHALL define a `DsqlOccConflictSpike` alert that fires when the rate of `tokeira_storage_dsql_occ_conflict_total` exceeds the configured threshold, defaulting to 50 per second, for the configured duration, defaulting to 1 minute.
3. THE Alert_Rule system SHALL define a `DsqlConnectionLeakDetected` alert that fires when `tokeira_dsql_connection_leak_suspects` is greater than 0 for the configured duration, defaulting to 5 minutes.
4. THE Alert_Rule system SHALL define a `DsqlRateLimiterThrottling` alert that fires when the rate of `tokeira_dsql_rate_limiter_throttled_total` exceeds the configured threshold, defaulting to 10 per second, for the configured duration, defaulting to 2 minutes.
5. THE Alert_Rule system SHALL define a `DsqlClassBudgetSaturation` alert that fires when any class has `tokeira_dsql_pool_class_saturation_ratio` exceeding the configured threshold, defaulting to 0.95, for the configured duration, defaulting to 3 minutes.
6. THE Alert_Rule system SHALL define a `ScrapeFailing` alert as described in Requirement 20.
7. THE Alert_Rule system SHALL define a `TelemetryIngestionStalled` alert as described in Requirement 20.
8. THE Alert_Rule system SHALL define a `ProjectionLagHigh` alert when projection lag exceeds the configured threshold for the configured duration.
9. THE Alert_Rule system SHALL define a `AutoscalerMetricsStale` alert when autoscaler metric freshness exceeds the configured staleness threshold.
10. THE Alert_Rule system SHALL define a `ControllerPlacementLoopFailing` alert when placement loop errors or generation CAS errors exceed configured thresholds.
11. THE Alert_Rule thresholds SHALL be generated from observability configuration defaults, allowing production deployments to tune thresholds without editing generated rule templates.
12. THE Alert_Rule system SHALL be provisioned as a Mimir ruler rules file in the compose platform and as a Prometheus-compatible rules file for non-compose deployments.
13. EVERY Alert_Rule SHALL include `severity`, `service`, `summary`, `description`, and `runbook_url` labels or annotations.
14. EVERY Alert_Rule SHALL avoid alert expressions that rely on high-cardinality unbounded labels.

### Requirement 25: Alert Runbooks

**User Story:** As an operator receiving an alert, I want a linked runbook with concrete diagnostics and safe remediation steps, so that I can respond quickly and consistently.

#### Acceptance Criteria

1. EVERY production Alert_Rule SHALL include a `runbook_url` annotation.
2. EVERY runbook SHALL describe likely causes, first diagnostic queries, relevant dashboard links, expected normal ranges, and safe remediation steps.
3. EVERY runbook SHALL identify which mitigations are safe for a solo operator and which require deeper investigation.
4. EVERY runbook SHALL include at least one Mimir query or dashboard panel reference for validating whether the alert is still active.
5. Dashboard panels referenced by runbooks SHALL use stable dashboard UIDs and panel references.
6. Runbook content SHALL be version-controlled with the observability spec and deployed artifacts.

### Requirement 26: Observability Smoke Test

**User Story:** As an operator, I want a command that verifies the deployed observability path end-to-end, so that I can know the monitoring stack works before relying on it.

#### Acceptance Criteria

1. THE `tkr observability check` command SHALL verify scrape target discovery for all expected Tokeira services.
2. THE `tkr observability check` command SHALL verify sample ingestion into Mimir for at least one metric from each expected Tokeira service.
3. THE `tkr observability check` command SHALL verify Loki log ingestion for at least one Structured_Log emitted during the check.
4. IF tracing is enabled, THE `tkr observability check` command SHALL verify trace export by emitting or locating a synthetic trace and confirming it is queryable in the configured trace backend.
5. THE `tkr observability check` command SHALL verify dashboard provisioning by confirming expected dashboard UIDs exist.
6. THE `tkr observability check` command SHALL verify alert rule loading by confirming expected rule groups exist in Mimir ruler or the configured rule backend.
7. THE command SHALL emit a synthetic metric and Structured_Log and verify both are queryable within a configurable timeout.
8. THE command SHALL return non-zero if any critical telemetry path is broken.
9. THE command SHALL print a concise diagnostic summary identifying which telemetry path failed: scrape, metrics ingestion, logs ingestion, trace ingestion, dashboard provisioning, or alert rule loading.

### Requirement 27: Deployment and Platform Integration

**User Story:** As an operator deploying Tokeira on ECS, I want the platform modules to provision observability configuration automatically, so that production telemetry works without manual Grafana, Alloy, Mimir, or Loki setup.

#### Acceptance Criteria

1. THE Compose_Platform SHALL provision Alloy, Mimir, Loki, Grafana, dashboards, and alert rules suitable for local production-like testing.
2. THE ECS_Platform SHALL provision or configure Alloy collection for all Tokeira services.
3. THE ECS_Platform SHALL expose or route `/metrics`, `/healthz`, and `/readyz` endpoints for each service according to the private networking model.
4. THE ECS_Platform SHALL configure service discovery or static target discovery so Alloy can scrape all expected process types.
5. THE ECS_Platform SHALL configure log collection for all Tokeira ECS services.
6. THE ECS_Platform SHALL configure Mimir and Loki targets or remote endpoints according to deployment configuration.
7. IF tracing is enabled, THE ECS_Platform SHALL configure the trace exporter endpoint and required credentials or network access.
8. THE platform apply operation SHALL fail with a clear error if observability is enabled but required endpoints, secrets, IAM permissions, or service discovery dependencies are missing.
9. THE platform modules SHALL avoid printing observability credentials, Grafana admin passwords, remote write credentials, or OTLP auth headers in logs or plan output.

### Requirement 28: Sensitive Data and Redaction

**User Story:** As an operator, I want observability data to avoid secrets and sensitive payloads, so that telemetry does not become a security liability.

#### Acceptance Criteria

1. NO metric label SHALL contain credentials, tokens, connection strings, SQL text, workflow payloads, request payloads, or raw user data.
2. NO Structured_Log SHALL contain credentials, tokens, connection strings, SQL text with bound values, workflow payloads, request payloads, or raw user data unless explicitly marked safe by the caller.
3. THE logging facade SHALL provide redaction helpers for fields that may contain sensitive values.
4. THE tracing instrumentation SHALL NOT attach workflow payloads, request payloads, raw SQL text, or secrets as span attributes.
5. IF an error contains a potentially sensitive message, THEN instrumentation SHALL emit a bounded `error_kind` and sanitized message rather than the raw error string.
6. THE Metric_Manifest and logging tests SHALL include checks or fixtures covering known sensitive field names.

### Requirement 29: Configuration and Defaults

**User Story:** As an operator, I want production observability to work with safe defaults while allowing thresholds and exporters to be tuned, so that small and large deployments can both use the same spec.

#### Acceptance Criteria

1. THE observability configuration SHALL enable Prometheus scrape endpoints by default for all production processes.
2. THE observability configuration SHALL disable OTLP metrics push by default unless explicitly configured.
3. THE observability configuration SHALL disable production trace export by default unless explicitly configured.
4. THE observability configuration SHALL enable JSON structured logs by default in production mode.
5. THE observability configuration SHALL provide configurable alert thresholds for all generated alert rules.
6. THE observability configuration SHALL provide configurable scrape intervals, evaluation intervals, OTLP buffer limits, OTLP retry backoff, trace sampling rate, and graceful flush timeout.
7. THE observability configuration SHALL provide documented defaults suitable for a small ECS deployment.
8. WHEN invalid observability configuration is supplied, THE process or platform apply command SHALL fail fast with a clear validation error.

### Requirement 30: Test Coverage and Validation

**User Story:** As a developer, I want observability artifacts and instrumentation contracts to be testable, so that regressions are caught before deployment.

#### Acceptance Criteria

1. Unit tests SHALL validate every Metric_Manifest for naming convention, label declaration, label cardinality classification, and metric type suffix consistency.
2. Unit tests SHALL validate that all metrics referenced by generated dashboards are declared in a Metric_Manifest or marked as external infrastructure metrics.
3. Unit tests SHALL validate that all metrics referenced by alert rules are declared in a Metric_Manifest or marked as external infrastructure metrics.
4. Unit tests SHALL validate that every alert rule includes severity, service, summary, description, and runbook URL metadata.
5. Unit tests SHALL validate that every dashboard passes Dashboard_Validator checks.
6. Integration tests SHOULD verify that a process can expose `/metrics`, `/healthz`, and `/readyz` endpoints.
7. Integration tests SHOULD verify that a synthetic request produces connected spans when tracing is enabled.
8. Integration tests SHOULD verify that logs emitted inside a span include `trace_id` and `span_id`.
9. Platform tests SHOULD verify that Compose provisions dashboards and alert rules automatically.
10. Platform tests SHOULD verify that ECS observability configuration includes scrape targets, log collection, and required endpoint wiring for every service type.

### Requirement 31: Metric Name Compatibility

**User Story:** As an operator, I want existing metrics to keep their stable names, so that dashboards, alert rules, and autoscaler queries do not silently break during observability hardening.

#### Acceptance Criteria

1. THE spec SHALL NOT rename existing metrics that are already emitted by production code and referenced by dashboards, alert rules, or autoscaler queries.
2. IF a spec metric name differs from an existing emitted metric name, THEN the existing emitted name SHALL be authoritative.
3. New metrics introduced by this spec SHALL follow the project naming convention and be declared in a Metric_Manifest.
4. Existing metrics SHALL be renamed only by an explicit deprecation or rename task that atomically updates all consumers, including dashboards, alert rules, smoke tests, runbooks, and autoscaler query references.
5. THE design SHALL maintain a compatibility table mapping any historical or proposed spec names to the authoritative emitted names.
6. THE compatibility table SHALL be generated or verified from source manifests, metric recording helpers, dashboard JSON, alert rules, and autoscaler query definitions.
