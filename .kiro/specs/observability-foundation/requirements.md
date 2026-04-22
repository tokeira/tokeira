# Requirements Document: Observability Foundation

## Introduction

This document captures the requirements for establishing Tokeira's observability foundation — the metrics, tracing, and structured logging infrastructure that every subsystem depends on. Currently Tokeira uses `tracing` for ad-hoc debug/warn/info logging (~63 call sites) and a custom in-memory `DeliveryMetrics` struct for broker poll success/timeout rates. There is no metrics registry, no Prometheus export, no OpenTelemetry integration, and no structured observability strategy.

This spec establishes the observability foundation BEFORE the DSQL storage work begins, so that every DSQL feature ships instrumented from day one. DSQL introduces fundamentally different performance characteristics (OCC conflicts, connection rate limits, commit latency) that require metrics to debug and operate.

The implementation is organized into 4 phases with explicit dependency ordering:

- Phase 1 (Metrics Registry + Prometheus Endpoint + Naming Conventions) — no dependencies
- Phase 2 (Baseline Metrics for Existing Subsystems) — depends on Phase 1
- Phase 3 (OpenTelemetry Tracing Integration + OTLP Export) — depends on Phase 1
- Phase 4 (Structured Logging Enhancements + Correlation IDs) — depends on Phase 3

Key design principles:

- Metrics are zero-cost when not observed (use `metrics` crate's lazy registration)
- Tracing spans add no measurable latency to the hot path
- Every new feature (especially DSQL) defines its metrics as part of the feature spec
- The observability foundation is a cross-cutting concern, not a standalone service
- Follow Temporal's metric naming where applicable for operator familiarity

The authoritative architecture documents are [005-decisions-and-boundaries](../../../docs/architecture/005-decisions-and-boundaries.md) and the performance targets therein.

## Glossary

- **Metrics_Registry**: A global registry backed by the `metrics` crate that records counters, gauges, and histograms. Exporters (Prometheus) read from this registry.
- **Prometheus_Exporter**: An HTTP endpoint (`/metrics`) that serves the metrics registry contents in Prometheus text exposition format for scraping.
- **OpenTelemetry**: A vendor-neutral observability framework providing APIs for distributed tracing, metrics, and logs. Tokeira uses the tracing integration (`tracing-opentelemetry`) for span propagation and OTLP export.
- **OTLP**: OpenTelemetry Protocol — the wire format for exporting traces, metrics, and logs to observability backends (Jaeger, Grafana Tempo, etc.).
- **Span**: A named, timed unit of work in a distributed trace. Spans form parent-child trees that represent request flow through the system.
- **Trace_Context**: Metadata (trace ID, span ID, sampling decision) propagated across service boundaries via gRPC metadata headers (`traceparent`, `tracestate`).
- **Correlation_ID**: A trace ID or request ID attached to log records, linking structured log output to the distributed trace that produced it.
- **Histogram**: A metric type that records the distribution of values (latencies, sizes) into configurable buckets. Used for percentile calculations.
- **Counter**: A monotonically increasing metric type. Used for counting events (requests, errors, transitions).
- **Gauge**: A metric type that can increase or decrease. Used for current-state measurements (active runs, open connections, queue depth).
- **DeliveryMetrics**: The existing custom in-memory counter struct in `tokeira-runtime/src/fairness.rs` that tracks broker poll success/timeout rates per queue. To be replaced by the metrics registry.
- **Kernel**: The pure deterministic state machine in `tokeira-kernel` that processes commands and produces transitions. Metrics: transitions committed, commands processed, events emitted.
- **Runtime**: The lane-based orchestration layer in `tokeira-runtime` that serializes commands, persists transitions, and publishes derived effects. Metrics: broker publish/poll/claim counts, lane submit latency, scanner tick counts, OCC retry counts.
- **Edge**: The gRPC transport layer in `tokeira-edge` that implements the Temporal WorkflowService API. Metrics: request counts, latency histograms, error rates per handler.
- **Storage**: The persistence layer in `tokeira-storage` that implements `RunRepository`. Metrics: commit_transition latency, load_run latency, history read latency.
- **Projection**: The visibility sink layer in `tokeira-projection` that maintains search-attribute tables. Metrics: projection lag, records processed, sink write latency.
- **Hot_Path**: The critical execution path from gRPC request receipt through kernel transition to storage commit. Instrumentation on this path must have negligible overhead.

## Requirements

---

## Phase 1: Metrics Registry, Prometheus Endpoint, and Naming Conventions

### Requirement 1.1: Global Metrics Registry

**User Story:** As a Tokeira developer, I want a global metrics registry backed by the `metrics` crate, so that all crates can record counters, gauges, and histograms through a single consistent interface.

#### Acceptance Criteria

1. THE Metrics_Registry SHALL use the `metrics` crate as the recording API across all Tokeira crates.
2. THE Metrics_Registry SHALL support counter, gauge, and histogram metric types.
3. THE Metrics_Registry SHALL use lazy registration so that metrics are zero-cost when no exporter is installed.
4. THE Metrics_Registry SHALL be initialized once during `tokeirad` startup before any subsystem begins processing.
5. WHEN no exporter is installed, THE Metrics_Registry SHALL discard all recorded values without allocating memory or acquiring locks.

### Requirement 1.2: Prometheus Scrape Endpoint

**User Story:** As a Tokeira operator, I want an HTTP `/metrics` endpoint that serves Prometheus text exposition format, so that I can scrape metrics into my existing monitoring stack.

#### Acceptance Criteria

1. THE Prometheus_Exporter SHALL expose an HTTP endpoint at a configurable address (default `0.0.0.0:9090`) serving the `/metrics` path.
2. THE Prometheus_Exporter SHALL render all registered metrics in Prometheus text exposition format.
3. THE Prometheus_Exporter SHALL run on a separate HTTP listener from the gRPC transport to avoid interference with the Temporal SDK wire protocol.
4. WHEN the Prometheus_Exporter is disabled via configuration, THE `tokeirad` process SHALL start without binding the metrics HTTP listener.
5. THE Prometheus_Exporter SHALL include `tokeira_build_info` as a gauge with `version`, `commit`, and `rustc_version` labels.

### Requirement 1.3: Metric Naming Convention

**User Story:** As a Tokeira developer, I want a documented metric naming convention, so that all crates produce consistent, discoverable metric names that follow Prometheus best practices and Temporal operator familiarity.

#### Acceptance Criteria

1. THE Metrics_Registry SHALL follow the naming pattern `tokeira_{crate}_{subsystem}_{metric}_{unit}` for all metrics (e.g., `tokeira_runtime_broker_publish_total`, `tokeira_edge_grpc_request_duration_seconds`). Each crate's `metrics.rs` SHALL export a `METRIC_NAMES` manifest that is validated against this convention in tests.
2. THE Metrics_Registry SHALL use `_total` suffix for counters, `_seconds` suffix for duration histograms, and `_bytes` suffix for size histograms.
3. THE Metrics_Registry SHALL define standard label names: `namespace` for the Temporal namespace, `task_queue` for the task queue name, `operation` for the operation type, and `status` for success/failure outcome.
4. THE Metrics_Registry SHALL document histogram bucket boundaries for latency metrics: `[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]` seconds.
5. THE Metrics_Registry SHALL document histogram bucket boundaries for size metrics: `[256, 1024, 4096, 16384, 65536, 262144, 1048576]` bytes.

### Requirement 1.4: Per-Crate Metric Definition Modules

**User Story:** As a Tokeira developer, I want each crate to define its metrics in a dedicated `metrics` module, so that metric names and descriptions are co-located with the code that records them and discoverable via code navigation.

#### Acceptance Criteria

1. WHEN a crate records metrics, THE crate SHALL define all metric names and descriptions in a `metrics.rs` module within that crate.
2. THE metric definition module SHALL export typed metric handles (counter, gauge, histogram) as lazy-initialized constants or functions.
3. THE metric definition module SHALL include a doc comment on each metric describing what it measures, its unit, and its labels.
4. THE metric definition module SHALL NOT depend on any specific exporter — only on the `metrics` crate recording API.

---

## Phase 2: Baseline Metrics for Existing Subsystems

### Requirement 2.1: Kernel Transition Metrics

**User Story:** As a Tokeira operator, I want metrics for kernel transitions, so that I can monitor the rate and distribution of workflow state changes.

#### Acceptance Criteria

1. WHEN the Kernel completes a transition, THE Kernel SHALL increment `tokeira_kernel_transition_committed_total` with labels `namespace` and `command_type`.
2. WHEN the Kernel emits history events, THE Kernel SHALL increment `tokeira_kernel_events_emitted_total` by the number of events, with label `event_type`.
3. WHEN the Kernel processes a command, THE Kernel SHALL increment `tokeira_kernel_commands_processed_total` with label `command_type`.

### Requirement 2.2: Runtime Broker Metrics

**User Story:** As a Tokeira operator, I want metrics for the delivery brokers, so that I can monitor task dispatch rates, sync-match efficiency, and queue depths.

#### Acceptance Criteria

1. WHEN a workflow task is published to the broker, THE Runtime SHALL increment `tokeira_runtime_broker_publish_total` with labels `namespace`, `task_queue`, and `task_type` (workflow or activity).
2. WHEN a poller receives a task (sync match), THE Runtime SHALL increment `tokeira_runtime_broker_sync_match_total` with labels `namespace`, `task_queue`, and `task_type`.
3. WHEN a task is published with no waiting poller (non-sync match), THE Runtime SHALL increment `tokeira_runtime_broker_non_sync_match_total` with labels `namespace`, `task_queue`, and `task_type`.
4. WHEN a poller long-poll times out without receiving a task, THE Runtime SHALL increment `tokeira_runtime_broker_poll_timeout_total` with labels `namespace`, `task_queue`, and `task_type`.
5. THE Runtime SHALL record `tokeira_runtime_broker_queue_depth` as a gauge reflecting the current number of ready tasks per queue, with labels `namespace`, `task_queue`, `task_type`, and `tier` (sticky or general).

### Requirement 2.3: Runtime Lane and Scanner Metrics

**User Story:** As a Tokeira operator, I want metrics for lane processing and scanner ticks, so that I can monitor runtime throughput and detect processing bottlenecks.

#### Acceptance Criteria

1. WHEN a lane processes a command submission, THE Runtime SHALL record `tokeira_runtime_lane_submit_duration_seconds` as a histogram with label `lane_id`.
2. WHEN a scanner completes a tick, THE Runtime SHALL increment `tokeira_runtime_scanner_tick_total` with labels `scanner_type` (timer, activity_timeout, workflow_timeout, wft_timeout, nexus_timeout) and `shard_id`.
3. WHEN a scanner tick finds work to dispatch, THE Runtime SHALL increment `tokeira_runtime_scanner_dispatched_total` with labels `scanner_type` and `shard_id`.
4. WHEN an OCC retry occurs during lane processing, THE Runtime SHALL increment `tokeira_runtime_occ_retry_total` with label `outcome` (success or exhausted).

### Requirement 2.4: Edge gRPC Handler Metrics

**User Story:** As a Tokeira operator, I want per-handler request count and latency metrics for the gRPC edge layer, so that I can monitor API usage patterns and detect slow handlers.

#### Acceptance Criteria

1. WHEN a gRPC handler completes, THE Edge SHALL increment `tokeira_edge_grpc_request_total` with labels `method` (e.g., `StartWorkflowExecution`, `PollWorkflowTaskQueue`), `namespace`, and `status` (ok, error, not_found, already_exists).
2. WHEN a gRPC handler completes, THE Edge SHALL record `tokeira_edge_grpc_request_duration_seconds` as a histogram with labels `method` and `namespace`.
3. WHEN a gRPC handler returns an error, THE Edge SHALL increment `tokeira_edge_grpc_error_total` with labels `method`, `namespace`, and `error_code` (the gRPC status code).
4. THE Edge SHALL record `tokeira_edge_grpc_active_requests` as a gauge reflecting the number of in-flight gRPC requests, with label `method`.

### Requirement 2.5: Storage Operation Metrics

**User Story:** As a Tokeira operator, I want latency and count metrics for storage operations, so that I can monitor persistence performance and detect degradation before it impacts workflows.

#### Acceptance Criteria

1. WHEN `commit_transition` completes, THE Storage SHALL record `tokeira_storage_commit_transition_duration_seconds` as a histogram with labels `namespace` and `outcome` (applied, conflict, duplicate).
2. WHEN `load_run` completes, THE Storage SHALL record `tokeira_storage_load_run_duration_seconds` as a histogram.
3. WHEN `read_history` completes, THE Storage SHALL record `tokeira_storage_read_history_duration_seconds` as a histogram.
4. THE Storage SHALL increment `tokeira_storage_operation_total` for each storage trait method call, with labels `operation` (commit_transition, load_run, read_history, resolve_execution, list_dispatchable_workflow_tasks, etc.) and `outcome` (success, error).

### Requirement 2.6: Projection Metrics

**User Story:** As a Tokeira operator, I want metrics for the projection pipeline, so that I can monitor visibility lag and detect sink write failures.

#### Acceptance Criteria

1. WHEN a projection worker processes a batch, THE Projection SHALL increment `tokeira_projection_records_processed_total` with label `partition_id`.
2. THE Projection SHALL record `tokeira_projection_lag_records` as a gauge reflecting the number of unprocessed projection log records per partition, with label `partition_id`.
3. WHEN a visibility sink write completes, THE Projection SHALL record `tokeira_projection_sink_write_duration_seconds` as a histogram with label `partition_id`.
4. WHEN a visibility sink write fails, THE Projection SHALL increment `tokeira_projection_sink_error_total` with label `partition_id`.

### Requirement 2.7: DeliveryMetrics Migration

**User Story:** As a Tokeira developer, I want the existing `DeliveryMetrics` struct replaced by the metrics registry, so that broker delivery statistics are exported via Prometheus alongside all other metrics.

#### Acceptance Criteria

1. WHEN the metrics registry is operational, THE Runtime SHALL record sync-match and non-sync-match counts through the `metrics` crate instead of the custom `DeliveryMetrics` struct.
2. THE Runtime SHALL preserve the existing fairness control loop behavior by reading metric values from the registry or a parallel in-memory snapshot.
3. WHEN the migration is complete, THE `DeliveryMetrics` struct SHALL be removed from `tokeira-runtime/src/fairness.rs`.

---

## Phase 3: OpenTelemetry Tracing Integration

### Requirement 3.1: OpenTelemetry Tracing Layer

**User Story:** As a Tokeira developer, I want OpenTelemetry tracing integrated with the existing `tracing` crate, so that spans are automatically created from `tracing` instrumentation and exported via OTLP.

#### Acceptance Criteria

1. THE Tracing_Layer SHALL use `tracing-opentelemetry` to bridge `tracing` spans to OpenTelemetry spans.
2. THE Tracing_Layer SHALL be installed as a `tracing-subscriber` layer during `tokeirad` startup, composable with the existing `fmt` and `EnvFilter` layers.
3. WHEN OTLP export is disabled via configuration, THE Tracing_Layer SHALL not be installed and existing `tracing` behavior SHALL remain unchanged.
4. THE Tracing_Layer SHALL propagate trace context (trace ID, span ID) across async task boundaries using `tracing`'s `Instrument` trait.

### Requirement 3.2: gRPC Trace Context Propagation

**User Story:** As a Tokeira operator, I want trace context extracted from incoming gRPC metadata and injected into outgoing calls, so that distributed traces span the full request lifecycle from SDK to server.

#### Acceptance Criteria

1. WHEN a gRPC request arrives with `traceparent` and `tracestate` metadata headers, THE Edge SHALL extract the W3C Trace Context and create a child span linked to the incoming trace.
2. WHEN a gRPC request arrives without trace context headers, THE Edge SHALL create a new root span for the request.
3. THE Edge SHALL attach the `namespace`, `workflow_id`, and `method` as span attributes on every gRPC handler span.
4. WHEN the Runtime makes outgoing HTTP calls via `NexusHttpClient` (Nexus operation dispatch), THE Runtime SHALL inject the current trace context into outgoing request headers. This injection happens at the `NexusHttpClient` boundary in `tokeira-runtime`, not in the edge layer, because Nexus HTTP dispatch is performed by `RuntimeDispatchPublisher`.

### Requirement 3.3: Span Conventions

**User Story:** As a Tokeira developer, I want consistent span naming and nesting conventions, so that traces are readable and comparable across different request types.

#### Acceptance Criteria

1. THE Edge SHALL create one span per gRPC handler invocation, named `grpc.{method}` (e.g., `grpc.StartWorkflowExecution`).
2. THE Runtime SHALL create one span per kernel transition, named `kernel.transition`, with attributes `command_type`, `run_key`, and `transition_seq`.
3. THE Storage SHALL create one span per storage operation, named `storage.{operation}` (e.g., `storage.commit_transition`, `storage.load_run`).
4. THE span hierarchy SHALL follow the call chain: `grpc.{method}` → `kernel.transition` → `storage.{operation}`, so that a single gRPC request produces a coherent trace tree.

### Requirement 3.4: OTLP Export Configuration

**User Story:** As a Tokeira operator, I want configurable OTLP export, so that I can send traces to my preferred observability backend (Jaeger, Grafana Tempo, AWS X-Ray).

#### Acceptance Criteria

1. THE OTLP_Exporter SHALL support configuring the OTLP endpoint via environment variable `TOKEIRA_OTLP_ENDPOINT` (default: `http://localhost:4317`).
2. THE OTLP_Exporter SHALL support configuring the export protocol via environment variable `TOKEIRA_OTLP_PROTOCOL` with values `grpc` (default) or `http`.
3. THE OTLP_Exporter SHALL support configuring the sampling rate via environment variable `TOKEIRA_TRACE_SAMPLE_RATE` (default: `1.0` for development, recommended `0.01` for production).
4. THE OTLP_Exporter SHALL batch spans and flush at configurable intervals (default: 5 seconds) to minimize export overhead.
5. IF the OTLP endpoint is unreachable, THEN THE OTLP_Exporter SHALL drop spans after a bounded retry and log a warning, without blocking the hot path.

---

## Phase 4: Structured Logging Enhancements

### Requirement 4.1: Correlation IDs in Log Records

**User Story:** As a Tokeira operator, I want every log record to include the current trace ID and span ID, so that I can correlate logs with distributed traces in my observability backend.

#### Acceptance Criteria

1. WHEN the OpenTelemetry tracing layer is active, THE Logging_Layer SHALL include `trace_id` and `span_id` fields in every log record that is emitted within a span context.
2. WHEN no span context is active, THE Logging_Layer SHALL omit `trace_id` and `span_id` fields rather than emitting empty values.
3. THE Logging_Layer SHALL include `trace_id` and `span_id` in both human-readable and JSON log formats.

### Requirement 4.2: JSON Log Format for Production

**User Story:** As a Tokeira operator, I want a JSON log format option, so that log aggregation systems (CloudWatch Logs, Loki, Datadog) can parse structured fields without custom regex patterns.

#### Acceptance Criteria

1. THE Logging_Layer SHALL support a JSON output format configurable via environment variable `TOKEIRA_LOG_FORMAT` with values `text` (default) or `json`.
2. WHEN JSON format is selected, THE Logging_Layer SHALL emit each log record as a single JSON object with fields: `timestamp`, `level`, `target`, `message`, `trace_id`, `span_id`, and any structured fields from the `tracing` span context.
3. WHEN text format is selected, THE Logging_Layer SHALL use the existing `tracing_subscriber::fmt` human-readable format.

### Requirement 4.3: Per-Module Log Level Configuration

**User Story:** As a Tokeira operator, I want per-module log level configuration, so that I can increase verbosity for specific subsystems during debugging without flooding logs from other subsystems.

#### Acceptance Criteria

1. THE Logging_Layer SHALL support per-module log level configuration via the `RUST_LOG` environment variable using `tracing_subscriber::EnvFilter` syntax (e.g., `tokeira_runtime=debug,tokeira_edge=info`).
2. THE Logging_Layer SHALL support runtime log level changes via a reload handle exposed through the Prometheus HTTP server as a `PUT /loglevel` endpoint. The endpoint accepts a `RUST_LOG`-compatible filter string in the request body and applies it via the `ReloadHandle`. This gives operators a concrete control surface without requiring process restart.
3. THE Logging_Layer SHALL default to `info` level for all Tokeira crates when `RUST_LOG` is not set.
