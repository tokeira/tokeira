# Implementation Plan: Observability Foundation

## Overview

Establish Tokeira's cross-cutting observability infrastructure in 4 phases: metrics registry + Prometheus export, baseline metrics for all subsystems, OpenTelemetry tracing with OTLP export, and structured logging enhancements. Each phase builds on the previous one. All new code uses the `metrics` crate recording API in library crates and exporter setup exclusively in `tokeirad`.

## Tasks

- [ ] 1. Phase 1 — Metrics Registry, Prometheus Endpoint, and Naming Conventions
  - [ ] 1.1 Add workspace dependencies to root `Cargo.toml`
    - Add `metrics`, `metrics-exporter-prometheus`, `tracing-opentelemetry`, `opentelemetry`, `opentelemetry-otlp`, `opentelemetry-sdk` as workspace dependencies
    - Add `proptest` as a workspace dev-dependency if not already present
    - _Requirements: 1.1.1, 1.4.4_

  - [ ] 1.2 Create `apps/tokeirad/src/observability.rs` — config and metrics initialization
    - Define `ObservabilityConfig` struct with fields: `metrics_enabled`, `metrics_addr`, `otlp_enabled`, `otlp_endpoint`, `otlp_protocol`, `trace_sample_rate`, `log_format`, `log_filter`
    - Implement `install_metrics()` that calls `PrometheusBuilder::new().install_recorder()` and records `tokeira_build_info` gauge with `version`, `commit`, `rustc_version` labels
    - Implement the Prometheus HTTP server (minimal hyper/axum handler calling `handle.render()` on GET `/metrics`) spawned as a dedicated Tokio task
    - When `metrics_enabled` is false, skip recorder installation and HTTP listener binding
    - _Requirements: 1.1.1, 1.1.2, 1.1.3, 1.1.4, 1.1.5, 1.2.1, 1.2.2, 1.2.3, 1.2.4, 1.2.5_

  - [ ] 1.3 Implement metric naming validation function
    - Create a shared validation function `validate_metric_name(name, metric_type) -> Result<(), NamingError>`
    - Enforce pattern `tokeira_{crate}_{subsystem}_{metric}_{unit}` with at least 4 segments after prefix
    - Enforce `_total` suffix for counters, `_seconds` for duration histograms, `_bytes` for size histograms
    - Define histogram bucket constants for latency `[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]` and size `[256, 1024, 4096, 16384, 65536, 262144, 1048576]`
    - Define standard label name constants: `namespace`, `task_queue`, `operation`, `status`
    - _Requirements: 1.3.1, 1.3.2, 1.3.3, 1.3.4, 1.3.5_

  - [ ]* 1.4 Write property test for metric name validation
    - **Property 1: Metric name validation**
    - Generate random strings with varying segment counts, prefixes, suffixes; include valid names, wrong prefixes, wrong suffixes, too few segments
    - Assert `validate_metric_name` accepts if and only if the name matches the convention and suffix matches the metric type
    - Use `proptest` crate, minimum 100 iterations
    - **Validates: Requirements 1.3.1, 1.3.2**

  - [ ]* 1.5 Write unit tests for Phase 1
    - Test `tokeira_build_info` gauge exists with correct labels after `install_metrics()`
    - Test histogram bucket constants match documented values
    - Test standard label name constants are defined
    - Test `tokeirad` does not bind metrics port when `metrics_enabled = false`
    - _Requirements: 1.2.4, 1.2.5, 1.3.4, 1.3.5_

- [ ] 2. Checkpoint — Ensure all Phase 1 tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 3. Phase 2 — Baseline Metrics for Existing Subsystems
  - [ ] 3.1 Create `crates/tokeira-runtime/src/metrics.rs` — runtime metric definitions
    - Define recording helper functions for broker metrics: `record_broker_publish`, `record_sync_match`, `record_non_sync_match`, `record_poll_timeout`, `set_queue_depth`
    - Define recording helpers for lane/scanner metrics: `record_lane_submit_duration`, `record_scanner_tick`, `record_scanner_dispatched`, `record_occ_retry`
    - Each function uses `metrics::counter!`, `metrics::gauge!`, or `metrics::histogram!` with the correct metric name and labels
    - Include doc comments on each metric describing what it measures, its unit, and its labels
    - Depend only on the `metrics` crate recording API
    - _Requirements: 2.2.1, 2.2.2, 2.2.3, 2.2.4, 2.2.5, 2.3.1, 2.3.2, 2.3.3, 2.3.4, 1.4.1, 1.4.2, 1.4.3, 1.4.4_

  - [ ] 3.2 Create `crates/tokeira-edge/src/metrics.rs` — edge metric definitions
    - Define recording helpers: `record_grpc_request`, `record_grpc_request_duration`, `record_grpc_error`, `set_grpc_active_requests`
    - Labels: `method`, `namespace`, `status`, `error_code`
    - Include doc comments; depend only on `metrics` crate
    - _Requirements: 2.4.1, 2.4.2, 2.4.3, 2.4.4, 1.4.1, 1.4.2, 1.4.3, 1.4.4_

  - [ ] 3.3 Create `crates/tokeira-storage/src/metrics.rs` — storage metric definitions
    - Define recording helpers: `record_commit_transition_duration`, `record_load_run_duration`, `record_read_history_duration`, `record_storage_operation`
    - Labels: `namespace`, `outcome`, `operation`
    - Include doc comments; depend only on `metrics` crate
    - _Requirements: 2.5.1, 2.5.2, 2.5.3, 2.5.4, 1.4.1, 1.4.2, 1.4.3, 1.4.4_

  - [ ] 3.4 Create `crates/tokeira-projection/src/metrics.rs` — projection metric definitions
    - Define recording helpers: `record_records_processed`, `set_projection_lag`, `record_sink_write_duration`, `record_sink_error`
    - Labels: `partition_id`
    - Include doc comments; depend only on `metrics` crate
    - _Requirements: 2.6.1, 2.6.2, 2.6.3, 2.6.4, 1.4.1, 1.4.2, 1.4.3, 1.4.4_

  - [ ] 3.5 Migrate `DeliveryMetrics` in `crates/tokeira-runtime/src/fairness.rs`
    - Replace `DeliveryMetrics::record_sync_match()` etc. with `metrics::counter!()` calls via the new `metrics.rs` module
    - Add a parallel `Arc<DashMap<QueueKey, QueueCounters>>` snapshot for the fairness control loop to read per-queue counts (since `metrics` crate doesn't expose per-label counter reads)
    - Remove `DeliveryMetrics` and `DeliveryMetricsInner` structs after wiring the parallel snapshot
    - _Requirements: 2.7.1, 2.7.2, 2.7.3_

  - [ ]* 3.6 Write property test for metric accounting accuracy
    - **Property 2: Metric accounting accuracy**
    - Generate random sequences of (operation_type, count) pairs; install a test recorder; replay the sequence; assert counters equal sum of increments, gauges equal last set value, histograms contain exactly N observations
    - Use `proptest` crate, minimum 100 iterations
    - **Validates: Requirements 2.1.2, 2.2.5, 2.4.4, 2.6.1**

  - [ ]* 3.7 Write property test for fairness control loop equivalence
    - **Property 3: Fairness control loop equivalence**
    - Reuse existing `arb_metrics()` strategy from `fairness.rs`; generate random `QueueMetricsSnapshot` values; compute drain shares via both old and new paths; assert equality
    - Use `proptest` crate, minimum 100 iterations
    - **Validates: Requirements 2.7.2**

  - [ ]* 3.8 Write unit tests for Phase 2 metrics modules
    - Test each recording helper emits the correct metric name and labels using a test recorder
    - Test `DeliveryMetrics` removal compiles cleanly and fairness control loop tests still pass
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7_

- [ ] 4. Checkpoint — Ensure all Phase 2 tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 5. Phase 3 — OpenTelemetry Tracing Integration
  - [ ] 5.1 Implement tracing subscriber composition in `apps/tokeirad/src/observability.rs`
    - Implement `install_tracing()` that builds a layered `tracing_subscriber::registry()` with `EnvFilter` (reloadable), `fmt` layer, and optional `tracing-opentelemetry` layer
    - Initialize OTLP tracer with configurable endpoint, protocol, sample rate, and batch flush interval
    - When `otlp_enabled` is false, skip the OpenTelemetry layer entirely
    - Return a `ReloadHandle` for runtime log level changes
    - _Requirements: 3.1.1, 3.1.2, 3.1.3, 3.1.4, 3.4.1, 3.4.2, 3.4.3, 3.4.4, 3.4.5_

  - [ ] 5.2 Modify `apps/tokeirad/src/main.rs` — replace tracing init with layered setup
    - Replace `tracing_subscriber::fmt().init()` with calls to `install_metrics()` then `install_tracing()` from `observability.rs`
    - Ensure recorder is installed before any `metrics::counter!()` call and subscriber before any `tracing::info!()` call
    - Spawn the Prometheus HTTP server task after recorder installation
    - _Requirements: 1.1.4, 3.1.2_

  - [ ] 5.3 Create `crates/tokeira-edge/src/grpc/tracing_interceptor.rs` — W3C trace context extraction
    - Implement `extract_trace_context()` using `TraceContextPropagator` to extract `traceparent`/`tracestate` from gRPC `MetadataMap`
    - Create a child span linked to the incoming trace when context is present; create a new root span when absent
    - Attach `namespace`, `workflow_id`, `method` as span attributes on every gRPC handler span
    - Implement trace context injection for outgoing calls (Nexus HTTP dispatch)
    - _Requirements: 3.2.1, 3.2.2, 3.2.3, 3.2.4_

  - [ ] 5.4 Add span instrumentation following naming conventions
    - Edge: one span per gRPC handler named `grpc.{method}`
    - Runtime: one span per kernel transition named `kernel.transition` with attributes `command_type`, `run_key`, `transition_seq`
    - Storage: one span per storage operation named `storage.{operation}`
    - Ensure span hierarchy follows `grpc.{method}` → `kernel.transition` → `storage.{operation}`
    - _Requirements: 3.3.1, 3.3.2, 3.3.3, 3.3.4_

  - [ ]* 5.5 Write property test for W3C trace context round-trip
    - **Property 4: W3C trace context extraction round-trip**
    - Generate random 16-byte trace IDs, 8-byte span IDs, and flag bytes; format as `traceparent`; extract via `extract_trace_context`; re-inject via `TraceContextPropagator`; assert trace ID and span ID are preserved
    - Use `proptest` crate, minimum 100 iterations
    - **Validates: Requirements 3.2.1**

  - [ ]* 5.6 Write unit tests for Phase 3
    - Test OTLP layer is not installed when `otlp_enabled = false`
    - Test root span creation when no `traceparent` header is present
    - Test span names match `grpc.{method}`, `kernel.transition`, `storage.{operation}` conventions
    - _Requirements: 3.1.3, 3.2.2, 3.3.1, 3.3.2, 3.3.3_

- [ ] 6. Checkpoint — Ensure all Phase 3 tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 7. Phase 4 — Structured Logging Enhancements
  - [ ] 7.1 Create `apps/tokeirad/src/correlation_layer.rs` — correlation ID layer
    - Implement a custom `tracing_subscriber::Layer` that reads the current OpenTelemetry span context and injects `trace_id` and `span_id` fields into log records
    - When no span context is active, omit `trace_id` and `span_id` fields
    - Ensure correlation IDs appear in both text and JSON log formats
    - Wire the correlation layer into the subscriber stack in `observability.rs`
    - _Requirements: 4.1.1, 4.1.2, 4.1.3_

  - [ ] 7.2 Add JSON log format support and per-module log levels
    - In `install_tracing()`, select `fmt::layer().json()` when `log_format` is `Json`, otherwise use default text format
    - Ensure JSON output emits each record as a single JSON object with fields: `timestamp`, `level`, `target`, `message`, `trace_id`, `span_id`, plus structured span fields
    - Wire `EnvFilter` with `RUST_LOG` support and default to `info` when unset
    - Expose the `ReloadHandle` for runtime log level changes without process restart
    - _Requirements: 4.2.1, 4.2.2, 4.2.3, 4.3.1, 4.3.2, 4.3.3_

  - [ ]* 7.3 Write property test for JSON log record validity
    - **Property 5: JSON log record validity**
    - Generate random log messages (including special characters, unicode, newlines); emit via `tracing::info!` with JSON format active; capture output; parse as JSON; assert required fields `timestamp`, `level`, `target`, `message` exist
    - Use `proptest` crate, minimum 100 iterations
    - **Validates: Requirements 4.2.2**

  - [ ]* 7.4 Write unit tests for Phase 4
    - Test text format is used when `TOKEIRA_LOG_FORMAT` is unset
    - Test `info` level is the default when `RUST_LOG` is unset
    - Test `trace_id`/`span_id` are absent from logs emitted outside a span context
    - Test runtime log level reload via the reload handle
    - _Requirements: 4.2.3, 4.3.1, 4.3.2, 4.3.3, 4.1.2_

- [ ] 8. Final checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation per phase
- Property tests validate the 5 correctness properties from the design document using `proptest`
- The `metrics` recorder MUST be installed before any subsystem startup; the tracing subscriber MUST replace `tracing_subscriber::fmt().init()` before any logging call
