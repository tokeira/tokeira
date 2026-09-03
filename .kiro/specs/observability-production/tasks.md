# Implementation Plan: Production Observability

## Overview

Implement production-facing observability for the full Tokeira deployment surface: `tokeirad`, `tokeira-controller`, `tokeira-autoscaler`, projection workers, and the supporting observability infrastructure used by Compose and ECS deployments.

This implementation turns the existing `tokeirad`-local observability code into a shared production observability layer, then applies it consistently across every production process. The work covers metric manifests and label governance, per-process `/metrics`/`/healthz`/`/readyz` endpoints, structured JSON logs, W3C trace propagation, configurable OTLP trace export, Phase 2 OTLP metrics design, DSQL operational metrics, controller/autoscaler/projection metrics, dashboard provisioning, alert rules, runbooks, platform integration, and an end-to-end `tkr observability check` command.

Target files and areas:

- `Cargo.toml` — add the shared observability crate to the workspace and wire dependencies.
- `crates/tokeira-observability/` — new shared crate for process telemetry, metric manifests, logging, tracing, readiness, redaction, and test helpers.
- `crates/tokeira-types/src/observability.rs` — preserve or re-export naming validation, metric type definitions, histogram buckets, and compatibility helpers.
- `apps/tokeirad/src/observability.rs`, `apps/tokeirad/src/bootstrap.rs`, `apps/tokeirad/src/config.rs`, `apps/tokeirad/src/correlation_format.rs` — migrate existing observability behaviour into the shared crate while preserving public behaviour.
- `apps/tokeira-controller/src/main.rs`, `apps/tokeira-controller/src/config.rs`, `apps/tokeira-autoscaler/src/main.rs`, and `apps/tokeirad` projection worker hosting code — install the shared observability runtime per process. Do not create a standalone projection binary in this spec.
- `crates/tokeira-storage/src/metrics.rs`, DSQL connection/reservoir/rate-limiter/migration code, and DSQL repository paths — add DSQL operational instrumentation.
- `crates/tokeira-edge/src/metrics.rs`, edge gRPC entry points, and trace interceptors — add edge metrics and trace attributes.
- `crates/tokeira-runtime/src/metrics.rs`, runtime lane/WFT/activity/query paths — add runtime metrics and spans.
- `crates/tokeira-controller/src/` — add placement, membership, generation CAS, drain, and budget metrics.
- `crates/tokeira-autoscaler/src/` — add loop, decision, stale-metric, active-reconciler, nomination, and Mimir query metrics.
- `crates/tokeira-projection/src/metrics.rs`, `worker.rs`, and projection sink/store paths — add projection throughput, lag, checkpoint, and sink metrics.
- `platforms/compose/`, `platforms/ecs/`, and/or existing platform generator crates such as `crates/tokeira-compose` and `crates/tokeira-aws` — provision Alloy scrape/log collection, dashboard JSON, alert rules, and runbooks.
- `apps/tkr/` — add `tkr observability check` smoke-test command.
- `docs/runbooks/observability/` — add production alert runbooks.

Ordering rationale: validation and test scaffolding first, then shared crate extraction, then per-process adoption, then instrumentation by subsystem, then dashboards/alerts/runbooks/platform integration, then end-to-end smoke testing and final workspace checkpoint. The shared observability crate must land before process adoption. Metric manifest and cardinality governance must land before adding new metrics at scale. Dashboards and alerts come after metrics exist, so queries can be validated against declared metric names.

## Tasks

- [x] 1. Establish observability contract tests and fixture validators
  - **Goal**: Create the validation harness before adding broad instrumentation, so Codex has an executable definition of the production observability contract.
  - Add a test helper module for observability validation under `crates/tokeira-observability/src/testing.rs` once the crate exists; until then, place provisional tests under the closest existing crate and move them in task 2.
  - Define fixture validators for:
    - metric naming and suffix rules;
    - metric label declarations and bounded-cardinality rules;
    - duplicate metric name detection across crate manifests;
    - dashboard JSON style validation;
    - alert rule YAML parsing and required labels/annotations;
    - runbook existence for every alert rule;
    - redaction of sensitive fields in log/config snapshots.
  - Validators MUST be deterministic and usable by unit tests and CI without requiring Mimir, Loki, Grafana, Alloy, or AWS.
  - Validators MUST emit actionable errors containing the metric/dashboard/alert/runbook file and offending field.
  - _Requirements: Metric Naming Validation, Metric Label Cardinality Governance, Dashboard Styling Conventions, Alerting Rules, Alert Runbooks, Redaction and Sensitive Data Safety_

  - [x]* 1.1 Add metric-manifest validation tests
    - Create tests that validate metric names start with `tokeira_`.
    - Create tests that reject counters not ending in `_total`.
    - Create tests that reject duration histograms not ending in `_seconds`.
    - Create tests that reject ratio gauges not ending in `_ratio` unless explicitly exempted.
    - Create tests that reject `*_build_info` unless the metric type is a gauge/info-style descriptor.
    - Create tests that reject duplicate metric names across manifests unless explicitly declared as the same shared metric.
    - Create tests that reject undeclared labels at recording helper boundaries where typed helpers exist.
    - Tests MUST pass against the current codebase. They validate existing metric names and labels as-is. Tests for newly introduced metrics belong in the same task that adds the metric and updates the manifest.
    - _Requirements: Metric Naming Validation, Metric Label Cardinality Governance_

  - [x]* 1.2 Add dashboard and alert artifact validation tests
    - Create a `DashboardValidator` test utility that parses dashboard JSON and checks the project styling conventions. This is a new test artifact, not an existing tool.
    - Add a test that discovers dashboard JSON files under `platforms/compose/dashboards/` and any existing dashboard directory used by the Compose generator.
    - Add a test that rejects time-series panels without `lineInterpolation: smooth`.
    - Add a test that rejects time-series panels with point markers enabled.
    - Add a test that rejects panels without descriptions.
    - Add a test that rejects panels without explicit units.
    - Add a test that rejects dashboards without a `$datasource` template variable.
    - Add a test that parses alert rule YAML files and verifies every alert has `severity`, `service`, `summary`, and `runbook_url` annotations.
    - Add a test that verifies every `runbook_url` points to a repository-local runbook file or a stable configured documentation base URL.
    - Tests MUST pass against current artifacts. When adding or changing dashboard and alert artifacts, update the artifacts and validators in the same task so every commit boundary remains green.
    - _Requirements: Dashboard Provisioning, Dashboard Styling Conventions, Alerting Rules, Alert Runbooks_

  - [x]* 1.3 Audit existing metric names and compatibility
    - Grep the codebase for metric constants, `counter!`, `gauge!`, and `histogram!` call sites.
    - Grep dashboard JSON, alert rules, and autoscaler query definitions for metric references.
    - Produce or update the design compatibility table mapping proposed/spec names to authoritative emitted names.
    - Where names diverge, update this spec to match the existing emitted names.
    - Do NOT rename existing metrics unless dashboard JSON, alert rules, smoke tests, runbooks, and autoscaler query references are updated atomically in the same commit.
    - _Requirements: Metric Name Compatibility, Metric Naming Validation_

- [x] 2. Add the shared `tokeira-observability` crate
  - Create `crates/tokeira-observability/Cargo.toml` and add it to the workspace.
  - Add modules:
    - `config.rs`
    - `metrics.rs`
    - `manifest.rs`
    - `labels.rs`
    - `http.rs`
    - `logging.rs`
    - `tracing.rs`
    - `readiness.rs`
    - `redaction.rs`
    - `shutdown.rs`
    - `testing.rs`
  - Keep the crate free of dependencies on runtime, storage, edge, controller, autoscaler, projection, or platform implementation crates.
  - The crate MAY depend on `tokeira-types` for shared metric validation and on `tokeira-config` only if doing so does not introduce a dependency cycle.
  - Add public API types from `design.md`:
    - `ProcessObservabilityConfig`
    - `ServiceName`
    - `LogFormat`
    - `OtlpMetricsConfig`
    - `TraceExportConfig`
    - `OtlpProtocol`
    - `ObservabilityRuntime`
    - `ObservabilityShutdown`
    - `install_observability(...)`
  - `install_observability` MUST validate manifests before installing recorder/subscriber state.
  - `install_observability` MUST return a clear error if invoked twice in the same process.
  - Preserve the single-recorder/single-subscriber constraint explicitly in code comments and error messages.
  - _Requirements: Per-Process Telemetry Surfaces, Prometheus Endpoint Completeness, Structured Logging, Trace Propagation and Export, Health and Readiness Endpoints_

  - [x]* 2.1 Unit tests for crate installation semantics
    - Test manifest validation runs before global recorder installation.
    - Test double-install returns a deterministic error.
    - Test disabled metrics mode does not bind `/metrics` but still permits logging/tracing setup.
    - Test disabled trace export does not require an OTLP endpoint.
    - Test missing required resource attributes in production config returns an error.
    - _Requirements: Per-Process Telemetry Surfaces, Redaction and Sensitive Data Safety_

- [x] 3. Implement typed metric manifest and cardinality governance
  - In `crates/tokeira-observability/src/manifest.rs`, implement a typed manifest model compatible with the design:
    - `MetricManifest`
    - `MetricDescriptor`
    - `MetricType`
    - `MetricLabel`
    - `LabelCardinality`
    - `MetricUnit`
  - Keep compatibility helpers for existing `METRIC_NAMES: &[(&str, MetricType)]` style manifests where required during migration.
  - Each descriptor MUST include:
    - metric name;
    - metric type;
    - semantic unit;
    - help/description text;
    - allowed labels;
    - label cardinality declaration;
    - owning component or crate.
  - Reject unbounded metric labels including `workflow_id`, `run_id`, `request_id`, `trace_id`, raw SQL text, raw error messages, node endpoint, ECS task ARN, and arbitrary error strings.
  - Permit `partition_id` only where the maximum configured partition count is bounded and documented by the descriptor.
  - Require labels such as `operation`, `outcome`, `class`, `loop`, `direction`, `reason`, and `error_kind` to be backed by enums or constrained constants.
  - Expose `validate_manifest(...)`, `validate_manifests(...)`, and `all_metric_names(...)` helpers.
  - _Requirements: Metric Naming Validation, Metric Label Cardinality Governance_

  - [x]* 3.1 Unit and property tests for manifest validation
    - Test every invalid suffix case is rejected.
    - Test duplicate metric names with incompatible descriptors are rejected.
    - Test duplicate metric names with identical shared descriptors are allowed only when marked shared.
    - Property-test generated metric names against `validate_metric_name()`.
    - Property-test label names for snake_case validation.
    - Test unbounded labels are rejected even when the metric name is valid.
    - _Requirements: Metric Naming Validation, Metric Label Cardinality Governance_

  - [x] 3.2 Classify existing metric labels
    - Classify `namespace` and `task_queue` as configuration-bounded labels and document expected operator-owned limits.
    - Classify `worker_instance_key` as unbounded-but-accepted with justification: the heartbeat store has a 1M entry cap, so metric cardinality is bounded by that cap.
    - Classify `partition_id`, `shard_id`, and `lane_id` as configuration-bounded labels with maximums derived from projection partition count, shard count, and lane count.
    - Keep truly unbounded labels such as `workflow_id`, `run_id`, `request_id`, `trace_id`, raw SQL text, and raw error strings rejected by validation.
    - _Requirements: Metric Label Cardinality Governance_

- [x] 4. Add typed label enums and safe recording helpers
  - In `crates/tokeira-observability/src/labels.rs`, add stable label enums and string conversions for common labels:
    - `DbClassLabel` (`control`, `commit`, `read`, `projection`, `maintenance`)
    - `StorageOperationLabel`
    - `RetryOutcomeLabel` (`success`, `exhausted`)
    - `ControllerCasOutcomeLabel` (`success`, `conflict`, `error`)
    - `AutoscalerLoopLabel` (`replica`, `scale_out`, `retirement`)
    - `ScalingDirectionLabel` (`up`, `down`, `hold`)
    - `NominationOutcomeLabel` (`accepted`, `rejected`, `timeout`)
    - `ProjectionOutcomeLabel`
    - `ProjectionErrorKindLabel`
    - `ServiceLabel` (`tokeirad`, `tokeira-controller`, `tokeira-autoscaler`, `alloy`, `mimir`, `loki`, `grafana`). Reserve `tokeira-projection` for a future standalone projection process; embedded projection metrics use the `tokeirad` service label.
  - Add helpers for common metrics operations that prevent arbitrary string labels in hot paths.
  - Update existing metric recorder helper modules to prefer typed label enums over ad-hoc strings.
  - Do not add workflow IDs, run IDs, request IDs, trace IDs, task ARNs, endpoint addresses, or raw error messages as metric labels.
  - _Requirements: Metric Label Cardinality Governance, DSQL Metrics, Controller Observability, Autoscaler Observability, Projection Worker Observability_

  - [x]* 4.1 Unit tests for typed labels and recording helpers
    - Test every enum serializes to the expected lowercase snake_case label value.
    - Test no enum variant serializes to an empty string.
    - Test recorder helpers expose only bounded labels.
    - Test invalid free-form labels are impossible or rejected before recording.
    - _Requirements: Metric Label Cardinality Governance_

- [x] 5. Migrate `tokeirad` observability into the shared crate
  - Move reusable logic from `apps/tokeirad/src/observability.rs` into `crates/tokeira-observability` without changing `tokeirad` operator-facing behaviour.
  - Preserve existing behaviour for:
    - Prometheus metrics installation;
    - `/metrics` endpoint;
    - `/config` endpoint if currently exposed;
    - `/loglevel` endpoint if currently exposed;
    - JSON/text log format selection;
    - existing trace correlation behaviour from `apps/tokeirad/src/correlation_format.rs`.
  - Replace app-local observability setup with a call to `install_observability(...)` from `apps/tokeirad/src/bootstrap.rs` or the current bootstrap entry point.
  - Ensure `tokeirad` passes manifests from edge, runtime, storage, projection, and any process-level metrics into installation.
  - Ensure `tokeirad` emits `tokeira_build_info` and process metadata gauges.
  - _Requirements: Prometheus Endpoint Completeness, Structured Logging, Trace Propagation and Export, Build and Process Metadata Metrics_

  - [x]* 5.1 Regression tests for `tokeirad` observability migration
    - Existing `apps/tokeirad` observability tests MUST continue to pass.
    - Add an integration test that starts the observability HTTP server and verifies `/metrics` returns HTTP 200.
    - Verify content type is `text/plain; version=0.0.4` or the exact Prometheus exposition content type currently expected by the repository.
    - Verify `/metrics` contains `tokeira_build_info` with `version`, `commit`, and `rustc_version` labels.
    - Verify a metric recorded through runtime/storage/edge helper APIs appears on the next scrape.
    - _Requirements: Prometheus Endpoint Completeness, Build and Process Metadata Metrics_

- [x] 6. Add per-process observability installation for controller, autoscaler, and embedded projection workers
  - Update `apps/tokeira-controller/src/main.rs` and `apps/tokeira-controller/src/config.rs` to configure and install `tokeira-observability`.
  - Update `apps/tokeira-autoscaler/src/main.rs` and relevant config modules to configure and install `tokeira-observability`.
  - Projection currently runs embedded in `tokeirad`; ensure `tokeirad` passes projection metric manifests to the shared observability runtime and exposes projection worker metrics from the `tokeirad` endpoint.
  - Do not create a standalone projection binary or wrapper in this spec.
  - Every production process MUST expose independent `/metrics`, `/healthz`, and `/readyz` endpoints.
  - Every process MUST identify itself with stable resource attributes:
    - `service`
    - `cluster`
    - `deployment`
    - `node_id` where applicable
    - `task_id` where applicable
    - version/commit/rustc metadata
  - The implementation MUST avoid a single aggregated in-process endpoint for multiple binaries; aggregation belongs to Alloy/Mimir.
  - _Requirements: Per-Process Telemetry Surfaces, Infrastructure Service Health Metrics, Health and Readiness Endpoints_

  - [x]* 6.1 Process startup tests for per-process observability
    - Test controller config can build a valid `ProcessObservabilityConfig`.
    - Test autoscaler config can build a valid `ProcessObservabilityConfig`.
    - Test `tokeirad` includes projection worker manifests in its process observability config.
    - Test each service name maps to the expected stable label value.
    - Test each process manifest validates successfully before startup.
    - _Requirements: Per-Process Telemetry Surfaces, Metric Naming Validation_

- [x] 7. Implement `/metrics`, `/healthz`, and `/readyz` HTTP endpoint behaviour
  - In `crates/tokeira-observability/src/http.rs`, implement a small HTTP server for process-local observability endpoints.
  - `/metrics` MUST return Prometheus exposition text when metrics are enabled.
  - `/healthz` MUST return process liveness without checking external dependencies.
  - `/readyz` MUST return dependency readiness and loop health as JSON.
  - `/readyz` MUST return non-2xx when any required readiness check is failing.
  - Keep `/config` and `/loglevel` behaviour compatible with existing `tokeirad` behaviour where those endpoints are already present.
  - The HTTP server MUST shut down gracefully when the process shutdown token is triggered.
  - _Requirements: Per-Process Telemetry Surfaces, Health and Readiness Endpoints, Redaction and Sensitive Data Safety_

  - [x] 7.1 Implement readiness registry and required process checks
    - In `crates/tokeira-observability/src/readiness.rs`, implement:
      - `ReadinessRegistry`
      - `ReadinessCheck`
      - `ReadinessStatus`
      - check update APIs for background loops.
    - `tokeirad` readiness MUST include storage availability, runtime loop state, and routing/ownership availability where applicable.
    - Controller readiness MUST include ability to read/write placement state and serve membership/routing streams.
    - Autoscaler readiness MUST include Mimir query availability and ECS/control-plane client availability when configured.
    - Projection readiness MUST include visibility sink availability and checkpoint store availability.
    - _Requirements: Health and Readiness Endpoints, Controller Observability, Autoscaler Observability, Projection Worker Observability_

  - [x]* 7.2 Endpoint and readiness tests
    - Test `/healthz` returns healthy when readiness checks are failing.
    - Test `/readyz` returns unhealthy JSON when a required check fails.
    - Test `/readyz` JSON includes check name, status, last update time, and message.
    - Test `/metrics` is unavailable or returns a clear error when metrics are disabled.
    - Test graceful shutdown stops the HTTP server without panicking.
    - _Requirements: Health and Readiness Endpoints, Prometheus Endpoint Completeness_

- [x] 8. Implement structured JSON logging and redaction
  - In `crates/tokeira-observability/src/logging.rs`, implement production JSON logging through `tracing_subscriber`.
  - Every production JSON log record MUST include:
    - timestamp;
    - level;
    - target;
    - service;
    - cluster;
    - deployment;
    - node_id or task_id where available.
  - When a current span exists, logs MUST include `trace_id` and `span_id`.
  - Workflow-related logs MAY include `namespace`, `workflow_type`, `run_id`, `shard_id`, and `bundle_id` as structured fields, but these MUST NOT become Loki labels in generated Alloy configuration.
  - Implement redaction helpers in `redaction.rs` for config snapshots, endpoint output, and log fields.
  - Redact at least:
    - passwords;
    - tokens;
    - secret ARNs if configured as sensitive;
    - credentials;
    - private keys;
    - connection strings with embedded credentials;
    - authorization headers.
  - Preserve text logging for local development where configured.
  - _Requirements: Structured Logging, Redaction and Sensitive Data Safety_

  - [x]* 8.1 Logging and redaction tests
    - Test production mode emits JSON logs.
    - Test log records include service and cluster fields.
    - Test span-scoped logs include trace correlation fields.
    - Test redaction masks known sensitive config keys.
    - Test high-cardinality fields remain fields and are not emitted as labels by generated Alloy config.
    - Test `/config` output, if enabled, redacts sensitive values.
    - _Requirements: Structured Logging, Redaction and Sensitive Data Safety_

- [x] 9. Implement W3C trace propagation, span export, and sampling controls
  - In `crates/tokeira-observability/src/tracing.rs`, centralise trace setup.
  - Install W3C TraceContext propagation for all production processes.
  - Preserve and generalise existing edge/runtime trace propagation code.
  - Add configurable OTLP span export using `TraceExportConfig`.
  - Add configurable head sampling.
  - Add error-biased sampling behaviour for traces containing:
    - storage commit errors;
    - OCC retry exhaustion;
    - `NotShardOwner` errors;
    - projection sink failures;
    - autoscaler reconciliation errors.
  - The implementation MAY start with always-sample-on-error markers if full tail sampling is not available in-process, but the limitation MUST be documented in code and design notes.
  - _Requirements: Trace Propagation and Export, Distributed Trace Attributes for Full Request Path_

  - [x] 9.1 Add required span boundaries and attributes
    - Edge gRPC root spans MUST include standard RPC attributes such as `rpc.system`, `rpc.service`, `rpc.method`, and `server.address` where available.
    - Edge gRPC root spans MUST include `tokeira.namespace` and `tokeira.request_id` when available.
    - Runtime dispatch spans MUST include `tokeira.lane_id`, `tokeira.shard_id`, `tokeira.bundle_id`, and `tokeira.command_type` where available.
    - Kernel invocation spans MUST include `tokeira.run_id`, `tokeira.workflow_type`, and `tokeira.transition_number` where available.
    - Storage commit spans MUST include `tokeira.storage_operation`, `tokeira.dsql_class`, and `tokeira.occ_retries`.
    - Avoid `tokeira.commit_duration_ms` span attributes unless explicitly needed; prefer span duration and metrics histograms.
    - _Requirements: Distributed Trace Attributes for Full Request Path, Trace Propagation and Export_

  - [x] 9.2 Audit span lifecycle safety
    - On hot/cancellable storage commit paths, use explicit `tracing::span!` creation around stable boundaries.
    - On hot/cancellable lane execution paths, use explicit span creation rather than broad `#[instrument]` on async functions.
    - Reserve `#[instrument]` for low-concurrency entry points such as gRPC handler entry, controller placement loop, and autoscaler control loop.
    - Document every intentional `#[instrument]` on async functions in hot-adjacent code.
    - _Requirements: Instrumentation Practice, Distributed Trace Attributes for Full Request Path_

  - [x]* 9.3 Trace propagation tests
    - Test inbound W3C trace headers are accepted by the gRPC tracing interceptor.
    - Test outbound runtime dispatch captures trace context for correlation.
    - Test the lane processing span includes `origin_trace_id` and `origin_span_id` attributes matching the dispatching span's context.
    - Test logs emitted inside a span include `trace_id` and `span_id`.
    - Test trace export disabled mode does not require an OTLP endpoint.
    - _Requirements: Trace Propagation and Export, Structured Logging_

  - [x] 9.4 Carry best-effort trace context through lane dispatch
    - Add `origin_trace_id: Option<[u8; 16]>` and `origin_span_id: Option<[u8; 8]>` to the lane command envelope rather than a `tracing::Span` handle.
    - On dispatch, capture the current trace ID and span ID as raw bytes from the OpenTelemetry context using `opentelemetry::trace::TraceContextExt` when available.
    - On receive, create a new `tracing::info_span!("lane.process", origin_trace_id = ..., origin_span_id = ...)` that records the IDs as hex-encoded string attributes.
    - Do NOT enter or hold the originating span across lane processing or any `.await` boundary.
    - Do NOT call `tracing::Span::follows_from` with reconstructed IDs; `tracing` cannot link spans from raw trace/span bytes.
    - Treat this as best-effort trace correlation across Tokio channels, not a correctness dependency and not a guarantee of strict parent-child continuity.
    - Test that a submitted command records origin trace attributes from edge dispatch through lane execution.
    - _Requirements: Trace Propagation and Export, Distributed Trace Attributes for Full Request Path_

- [ ] 10. Phase 2: Implement OTLP metrics push export
  - This is not a Phase 1 prerequisite. Phase 1 completes Prometheus scrape coverage and manifest validation.
  - Prefer an Alloy remote-write bridge to an OTLP-compatible endpoint because it requires no Rust recorder fanout.
  - If Rust-side push remains necessary, design a `metrics-util` fanout recorder or a scrape-and-push bridge before implementing.
  - In `crates/tokeira-observability/src/metrics.rs`, add configurable OTLP metrics export only after the selected bridge/fanout design is approved.
  - OTLP metrics export MUST be disabled by default.
  - When Phase 2 is implemented and enabled, it MUST export metrics declared in the process metric manifests and recorded through the shared metrics facade.
  - Exporter configuration MUST support gRPC and HTTP protocols where supported by dependencies.
  - Implement bounded in-memory buffering up to `max_buffered_batches`.
  - On buffer overflow, drop the oldest batch and increment `tokeira_otlp_metrics_dropped_batches_total`.
  - On unreachable endpoint, retry with exponential backoff.
  - On graceful shutdown, flush pending metric batches bounded by `shutdown_flush_timeout`.
  - _Requirements: OTLP Metrics Push Export_

  - [ ]* 10.1 OTLP metrics exporter tests
    - These tests are Phase 2 and are not required for the Phase 1 checkpoint.
    - Test disabled mode does not spawn an exporter.
    - Test enabled mode requires endpoint configuration.
    - Test unreachable endpoint increments retry/drop counters through a fake exporter implementation.
    - Test buffer overflow drops the oldest batch.
    - Test graceful shutdown calls flush with a bounded timeout.
    - _Requirements: OTLP Metrics Push Export_

- [x] 11. Add build and process metadata metrics
  - Register `tokeira_build_info` gauge with labels:
    - `version`
    - `commit`
    - `rustc_version`
  - Add process metadata gauges or info metrics for:
    - service name;
    - cluster;
    - deployment;
    - node/task identity where available.
  - Ensure metadata metrics use bounded labels and do not include high-cardinality task ARNs unless explicitly normalised to a bounded task ID field.
  - Use existing build metadata crate/helpers where present.
  - _Requirements: Prometheus Endpoint Completeness, Build and Process Metadata Metrics, Metric Label Cardinality Governance_

  - [x]* 11.1 Metadata metrics tests
    - Test `tokeira_build_info` appears in every process manifest or shared process manifest.
    - Test version/commit/rustc labels are present.
    - Test metadata metrics pass naming and label validation.
    - _Requirements: Prometheus Endpoint Completeness, Metric Naming Validation_

- [x] 12. Instrument DSQL OCC conflicts, retries, and operation latency
  - Update `crates/tokeira-storage/src/metrics.rs` with typed descriptors and recording helpers for DSQL operation metrics.
  - Instrument all DSQL repository operations that can encounter OCC conflicts or retries.
  - Increment `tokeira_storage_dsql_occ_conflict_total` with bounded `operation` label when SQLSTATE `40001` is observed.
  - Record retry attempts in the existing `tokeira_storage_dsql_commit_retries` histogram.
  - Increment `tokeira_storage_dsql_retry_total` with bounded `operation` and `outcome` labels.
  - Record operation latency histograms using `_seconds` suffix.
  - Do not include raw SQL text, workflow IDs, run IDs, or raw error messages as labels.
  - _Requirements: OCC Conflict Counters and Retry Histograms, Metric Label Cardinality Governance, Distributed Trace Attributes for Full Request Path_

  - [x]* 12.1 DSQL OCC and retry tests
    - Unit-test SQLSTATE `40001` classification maps to OCC conflict metrics.
    - Test successful retry records retry count and `outcome=success`.
    - Test retry exhaustion increments `outcome=exhausted`.
    - Test operation labels are bounded enum values.
    - Test no raw SQL or error message appears in metric labels.
    - _Requirements: OCC Conflict Counters and Retry Histograms, Metric Label Cardinality Governance_

- [x] 13. Instrument DSQL migrations
  - Instrument DSQL schema migration execution code.
  - On migration success, emit an INFO structured log with migration filename, duration, and resulting schema version.
  - On migration failure, emit an ERROR structured log with migration filename, error class, and SQLSTATE code when available.
  - Increment `tokeira_storage_migration_applied_total` with `status=success|failure`.
  - Record `tokeira_storage_migration_duration_seconds` histogram.
  - Ensure migration filename is a log field, not a metric label unless the label set is explicitly bounded and accepted by the manifest validator.
  - _Requirements: Migration Event Observability, Structured Logging, Metric Label Cardinality Governance_

  - [x]* 13.1 Migration observability tests
    - Test successful migration emits success counter and duration histogram.
    - Test failed migration emits failure counter and structured error log.
    - Test SQLSTATE is logged when available.
    - Test migration filename is not used as an unbounded metric label.
    - _Requirements: Migration Event Observability_

- [x] 14. Instrument DSQL reservoir, rate limiter, class budgets, and leak detection
  - Add metrics for reservoir state:
    - `tokeira_dsql_reservoir_ready_connections`
    - `tokeira_dsql_reservoir_in_flight`
    - `tokeira_dsql_reservoir_utilization_ratio`
    - `tokeira_dsql_reservoir_target_connections`
    - `tokeira_dsql_pool_connections_total`
    - `tokeira_dsql_pool_empty_reservoir_total`
    - `tokeira_dsql_reservoir_refill_errors_total`
    - `tokeira_dsql_reservoir_refill_duration_seconds`
  - Add metrics for the rate limiter:
    - `tokeira_dsql_rate_limiter_tokens_remaining`
    - `tokeira_dsql_rate_limiter_throttled_total`
    - `tokeira_dsql_rate_limiter_throttle_duration_seconds`
    - `tokeira_dsql_pool_rate_limiter_rate`
  - Add metrics for class budgets:
    - `tokeira_dsql_pool_class_budget_total`
    - `tokeira_dsql_pool_class_in_use`
    - `tokeira_dsql_pool_class_waiters`
    - `tokeira_dsql_class_permit_wait_duration_seconds`
  - Add leak detection metrics:
    - `tokeira_dsql_connection_leak_detected_total`
    - `tokeira_dsql_connection_leak_suspects`
    - `tokeira_dsql_connection_checkout_overdue_seconds`
  - Leak detection MUST use an explicit low-cardinality checkout call-site ID, not a captured stack trace as a metric label.
  - Emit structured warning logs when reservoir utilization exceeds 0.8; class warnings follow the pending-wait pressure policy in task 14.2.
  - _Requirements: Connection Leak Detection, DSQL Reservoir Depth Metrics, Rate-Limiter Token Metrics, Class-Budget Saturation Metrics_

  - [x]* 14.1 Reservoir/rate/class/leak tests
    - Test checkout increments in-flight and decreases ready count.
    - Test return decrements in-flight and updates utilization ratio.
    - Test empty reservoir increments `tokeira_dsql_pool_empty_reservoir_total`.
    - Test throttled connection creation increments throttled counter and records wait duration.
    - Test class permit waiters and in-use counts update under contention.
    - Test leak suspicion increments gauge/counter after deadline.
    - Test returning an overdue connection decrements suspects gauge and records overdue duration.
    - Test checkout call-site identifier is bounded and not a raw backtrace label.
    - _Requirements: Connection Leak Detection, DSQL Reservoir Depth Metrics, Rate-Limiter Token Metrics, Class-Budget Saturation Metrics_

  - [x] 14.2 Bound class-budget pressure warnings
    - Track pending semaphore acquisitions with cancellation-safe registrations and preserve per-class warning history across recovery and reconfiguration.
    - Warn on an observed pending wait of at least 5 seconds, with at least 60 seconds between warnings per class. Use the existing reporter so blocked acquisitions remain observable.
    - Preserve utilization reporting and embedded defaults; document the pressure condition at the warning site.
    - _Requirements: 14.5, 14.7–14.11_

  - [x] 14.3 Verify class-budget pressure policy
    - Implement the required Property 4 state-machine test with at least 100 generated waiter-lifecycle sequences and an independent reference model.
    - Test transient contention, exact threshold/cooldown boundaries, sustained blocked acquisitions, bounded repetition, recovery, independent classes, cancellation, semaphore closure, and reconfiguration.
    - Run the DSQL-enabled storage tests and the root finish bar after implementation.
    - _Requirements: 14.5, 14.7–14.11_

- [x] 15. Instrument edge and runtime paths
  - Update `crates/tokeira-edge/src/metrics.rs` and gRPC entry points.
  - Add or verify bounded metrics for:
    - request counts by method, namespace, and status;
    - request duration by method;
    - `NotShardOwner` recovery/invalidation events;
    - poll admission/broker dispatch where applicable.
  - Update `crates/tokeira-runtime/src/metrics.rs`, lane execution, workflow task, activity, query, and timeout paths.
  - Add or verify bounded metrics for:
    - lane processing duration by command type;
    - WFT start/completion/failure/timeout outcomes;
    - activity task start/completion/failure/retry/timeout outcomes;
    - buffered query counts and query latency;
    - runtime queue depth or dispatch backlog where available.
  - Ensure high-cardinality workflow/run identifiers are emitted as span/log fields only, not metric labels.
  - Add a `service` label to edge gRPC metrics only as an explicit label addition:
    - preserve existing `method`, `namespace`, and `status` labels on `tokeira_edge_grpc_request_total`;
    - preserve existing `method` and `namespace` labels on `tokeira_edge_grpc_request_duration_seconds`;
    - update all dashboard panels and alert rules that reference these metrics to tolerate the added label dimension;
    - update metric manifest descriptors in the same change that emits the new label;
    - verify existing aggregate queries still work because they aggregate across the new label value.
  - _Requirements: Edge and Runtime Observability, Dashboard Provisioning, Distributed Trace Attributes for Full Request Path, Metric Label Cardinality Governance_

  - [x]* 15.1 Edge/runtime instrumentation tests
    - Test gRPC request metrics preserve existing bounded `method`, `namespace`, and `status` labels.
    - Test any added edge gRPC `service` label does not break existing aggregate dashboard queries.
    - Test `NotShardOwner` recovery increments a bounded counter.
    - Test lane processing duration records command type using an enum-backed label.
    - Test workflow/run IDs do not appear as metric labels.
    - Test query buffering metrics update on buffered and direct query paths.
    - _Requirements: Edge and Runtime Observability, Metric Label Cardinality Governance_

- [x] 16. Instrument placement controller
  - Update `crates/tokeira-controller/src/` and add `metrics.rs` if not already present.
  - Add descriptors and recording helpers for:
    - `tokeira_controller_placement_loop_duration_seconds`
    - `tokeira_controller_generation_cas_total` with `outcome=success|conflict|error`
    - `tokeira_controller_routing_snapshot_size`
    - `tokeira_controller_bundle_ownership_churn_total`
    - `tokeira_controller_drain_active_nodes`
    - `tokeira_controller_budget_allocation_total` with bounded `outcome`
    - `tokeira_controller_membership_nodes_total`
  - Instrument placement loop execution duration.
  - Instrument generation CAS attempts.
  - Instrument routing snapshot generation and snapshot size.
  - Instrument bundle owner changes.
  - Instrument drain state transitions and active drain count.
  - Instrument connection budget allocation CAS attempts.
  - _Requirements: Placement Controller Observability, Controller Dashboard_

  - [x]* 16.1 Controller observability tests
    - Test placement loop records duration once per cycle.
    - Test generation CAS success/conflict/error outcomes are recorded.
    - Test routing snapshot size equals number of ownership entries.
    - Test owner change increments churn counter.
    - Test drain-active gauge reflects active drain nodes.
    - Test membership gauge reflects registered runtime nodes.
    - _Requirements: Placement Controller Observability_

- [x] 17. Instrument autoscaler
  - Update `crates/tokeira-autoscaler/src/` metrics and control-loop code.
  - Prefer the name `active_reconciler` over generic `leader` in new code unless compatibility requires existing names.
  - Add descriptors and recording helpers for:
    - `tokeira_autoscaler_loop_duration_seconds` with `loop=replica|scale_out|retirement`
    - `tokeira_autoscaler_scaling_decisions_total` with `loop`, `direction=up|down|hold`, and bounded `reason`
    - `tokeira_autoscaler_metric_freshness_age_seconds`
    - `tokeira_autoscaler_stale_metrics_total`
    - `tokeira_autoscaler_service_desired_replicas` with configuration-bounded `service`
    - `tokeira_autoscaler_scale_in_nomination_total` with `outcome=accepted|rejected|timeout`
    - `tokeira_autoscaler_active_reconciler_lease_held` or the existing compatibility name if required
    - `tokeira_autoscaler_mimir_query_duration_seconds`
  - Suppress scale-in decisions when metric freshness exceeds the configured threshold and increment stale metrics counter.
  - Ensure `reason` values are enum-backed and bounded.
  - _Requirements: Autoscaler Observability, Autoscaler Dashboard, Metric Label Cardinality Governance_

  - [x]* 17.1 Autoscaler observability tests
    - Test each control loop records duration with the correct loop label.
    - Test scaling decisions record direction and bounded reason.
    - Test stale metrics increment stale counter and suppress scale-in.
    - Test desired replica gauge records per bounded service label.
    - Test nomination outcomes record accepted/rejected/timeout.
    - Test active reconciler gauge is 1 when held and 0 otherwise.
    - Test Mimir query duration records on success and failure.
    - _Requirements: Autoscaler Observability_

- [x] 18. Instrument projection workers
  - Update `crates/tokeira-projection/src/metrics.rs`, `worker.rs`, checkpoint handling, and sink/store code.
  - Add descriptors and helpers for:
    - existing `tokeira_projection_records_processed_total` with bounded `partition_id` and a new bounded `outcome` label
    - existing `tokeira_projection_worker_lag_records` with bounded `partition_id`
    - existing `tokeira_projection_sink_write_duration_seconds` with bounded `partition_id`
    - existing `tokeira_projection_sink_error_total` with bounded `partition_id` and a new enum-backed `error_kind` label
    - `tokeira_projection_checkpoint_lag_seconds`
    - `tokeira_projection_checkpoint_transition_sequence`
    - `tokeira_projection_latest_transition_sequence`
    - `tokeira_projection_worker_batch_records`
    - `tokeira_projection_poll_empty_total`
  - Ensure `partition_id` usage declares the configured upper bound in the manifest descriptor.
  - Update the projection metric helpers and manifest in the same task when adding the `outcome` and `error_kind` labels.
  - Update projection dashboard panels and alert rules to use the new label dimensions where useful, and verify existing dashboard queries still render correctly because they aggregate across the new labels.
  - Emit structured logs with partition, checkpoint, and sink error information, while keeping raw error details out of metric labels.
  - _Requirements: Projection Worker Observability, Projection Worker Dashboard, Metric Label Cardinality Governance_

  - [x]* 18.1 Projection observability tests
    - Test processed records counter increments by outcome.
    - Test lag records gauge reflects cursor-to-latest distance.
    - Test batch apply duration records histogram samples.
    - Test sink errors use bounded error kind labels.
    - Test checkpoint lag updates after successful checkpoint writes.
    - Test `partition_id` manifest descriptor declares bounded cardinality.
    - _Requirements: Projection Worker Observability, Metric Label Cardinality Governance_

- [x] 19. Implement infrastructure telemetry scraping and health metrics
  - Update Compose and ECS observability platform generation to scrape infrastructure services:
    - Alloy self-scrape;
    - Mimir `/metrics`;
    - Loki `/metrics`;
    - Grafana health/datasource status where available.
  - Alloy scrape configuration MUST include one scrape job per Tokeira process type and one scrape job per infrastructure service type.
  - Generated scrape labels MUST include stable bounded labels such as `service`, `cluster`, `deployment`, and `target_kind`.
  - Do not place `run_id`, `workflow_id`, `request_id`, `trace_id`, or ECS task ARN in scrape labels.
  - _Requirements: Infrastructure Service Health Metrics, Per-Process Telemetry Surfaces, Metric Label Cardinality Governance_

  - [x]* 19.1 Infrastructure telemetry config tests
    - Test Alloy config includes self-scrape.
    - Test Alloy config includes Mimir, Loki, and Grafana checks where those services are enabled.
    - Test scrape configs include every Tokeira process type.
    - Test generated labels are bounded and do not include forbidden high-cardinality labels.
    - Test private-only ECS configs use private endpoints and service discovery names.
    - _Requirements: Infrastructure Service Health Metrics, Platform Integration_

- [x] 20. Implement dashboard provisioning artifacts
  - Add or update dashboard JSON files under `platforms/compose/dashboards/` and the dashboard source directory used by the platform generator.
  - Provision these dashboards:
    - `DSQL Connection Health`
    - `OCC Contention`
    - updated `broker-runtime-health`
    - updated `storage-projection-health`
    - `Placement Controller`
    - `Autoscaler`
    - `Projection Workers`
    - `Infrastructure Health`
  - Every dashboard MUST include `$datasource` variable defaulting to the Mimir datasource.
  - Every dashboard MUST follow the style contract:
    - smooth line interpolation for time-series panels;
    - no point markers;
    - explicit units;
    - panel descriptions;
    - meaningful legends;
    - row-based organisation with collapsed secondary rows.
  - Dashboards MUST use declared metric names only.
  - Dashboards MUST avoid raw high-cardinality labels in legends.
  - _Requirements: Dashboard Provisioning, Controller Dashboard, Autoscaler Dashboard, Projection Worker Dashboard, Dashboard Styling Conventions_

  - [x]* 20.1 Dashboard validation tests
    - Run the dashboard validator from task 1 against every dashboard JSON file.
    - Test every dashboard parses as valid Grafana JSON.
    - Test every metric referenced in dashboard expressions exists in a manifest or is an approved infrastructure metric.
    - Test each required dashboard title is present.
    - Test each required row title is present for controller, autoscaler, and projection dashboards.
    - _Requirements: Dashboard Provisioning, Dashboard Styling Conventions_

- [x] 21. Implement alert rules and runbooks
  - Add Prometheus/Mimir-compatible alert rules for:
    - `DsqlReservoirExhaustion`
    - `DsqlOccConflictSpike`
    - `DsqlConnectionLeakDetected`
    - `DsqlRateLimiterThrottling`
    - `DsqlClassBudgetSaturation`
    - `ScrapeFailing`
    - `TelemetryIngestionStalled`
    - controller generation CAS failures/conflicts;
    - controller ownership churn spikes;
    - autoscaler stale metrics;
    - autoscaler active reconciler absence or contention;
    - projection checkpoint lag;
    - projection sink error rate.
  - Alert thresholds MUST be generated from observability configuration defaults rather than hard-coded only in static YAML.
  - Every alert MUST include:
    - `severity`;
    - `service`;
    - `summary`;
    - `description`;
    - `runbook_url`.
  - Add runbooks under `docs/runbooks/observability/` for every production alert.
  - Each runbook MUST include:
    - meaning of the alert;
    - likely causes;
    - first dashboard to open;
    - first PromQL/log queries to run;
    - safe remediation steps;
    - escalation guidance;
    - related alerts.
  - _Requirements: Alerting Rules, Alert Runbooks, Infrastructure Service Health Metrics_

  - [x]* 21.1 Alert and runbook validation tests
    - Test alert YAML parses successfully.
    - Test every alert references declared metrics or approved infrastructure metrics.
    - Test every alert includes required labels and annotations.
    - Test every alert has a corresponding runbook file.
    - Test generated thresholds can be overridden by config.
    - Test alert expressions avoid high-cardinality grouping.
    - _Requirements: Alerting Rules, Alert Runbooks, Metric Label Cardinality Governance_

- [x] 22. Add observability configuration model and defaults
  - Extend the production configuration model with an `observability` section that can configure:
    - metrics enabled/disabled;
    - metrics bind address/port;
    - JSON/text log format;
    - log filter;
    - Phase 2 OTLP metrics settings, validated but disabled by default;
    - trace export settings;
    - sampling rate;
    - leak detection deadline;
    - alert thresholds;
    - dashboard provisioning enablement;
    - runbook base URL;
    - smoke-test behaviour.
  - Provide defaults suitable for local Compose and production ECS.
  - Validate invalid combinations, such as Phase 2 OTLP metrics enabled without endpoint.
  - Ensure sensitive values are marked for redaction.
  - _Requirements: Configuration Defaults, Trace Propagation and Export, Redaction and Sensitive Data Safety_

  - [x]* 22.1 Configuration tests
    - Test default config is valid for Compose.
    - Test default config is valid for ECS.
    - Test OTLP enabled without endpoint fails validation.
    - Test sample rate outside `0.0..=1.0` fails validation.
    - Test alert thresholds can be overridden.
    - Test sensitive fields are redacted from debug/config output.
    - _Requirements: Configuration Defaults, Redaction and Sensitive Data Safety_

- [x] 23. Integrate observability with Compose platform
  - Update Compose platform generation so `tkr infra apply` provisions:
    - dashboard JSON files;
    - alert rule files;
    - runbooks or runbook URL mappings;
    - Alloy scrape configuration for all Tokeira processes and infrastructure services;
    - Alloy log collection configuration forwarding to Loki;
    - optional OTLP trace forwarding if configured.
  - When a new dashboard JSON file is added to the dashboard directory, Compose MUST provision it on the next apply.
  - Ensure generated Compose service definitions expose `/metrics`, `/healthz`, and `/readyz` ports as appropriate for local development.
  - Ensure local development remains usable when trace export is disabled.
  - _Requirements: Compose Platform Integration, Dashboard Provisioning, Alerting Rules, Infrastructure Service Health Metrics_

  - [x]* 23.1 Compose integration tests
    - Test dashboard file discovery includes newly added dashboard files.
    - Test generated Grafana provisioning references all dashboards.
    - Test generated Mimir/Prometheus rules include all alert groups.
    - Test generated Alloy config scrapes all enabled services.
    - Test generated Alloy config forwards logs to Loki.
    - Test disabling trace export removes trace backend requirements.
    - _Requirements: Compose Platform Integration, Dashboard Provisioning, Alerting Rules_

- [x] 24. Integrate observability with ECS platform
  - Update ECS platform generation so services expose and are discoverable by Alloy for:
    - `/metrics`;
    - `/healthz`;
    - `/readyz`.
  - Ensure ECS task definitions include observability environment/config values.
  - Ensure Alloy can discover/scrape each service using the chosen ECS service discovery model.
  - Ensure log collection is configured for all Tokeira process containers and infrastructure containers.
  - Ensure generated IAM/task roles allow only the observability actions required by the platform.
  - Ensure private-only deployments do not require public observability endpoints.
  - Ensure dashboard and alert artifacts are included in ECS observability provisioning where Grafana/Mimir are deployed by the platform.
  - _Requirements: ECS Platform Integration, Per-Process Telemetry Surfaces, Structured Logging, Infrastructure Service Health Metrics_

  - [x]* 24.1 ECS integration tests
    - Test task definitions include observability config and port mappings where required.
    - Test Alloy scrape config can discover every service type.
    - Test log collection includes every process container.
    - Test no public ingress is required for metrics/logs/traces in private-only ECS mode.
    - Test generated IAM policies do not include broad wildcard permissions unless already required by the platform and documented.
    - _Requirements: ECS Platform Integration, Redaction and Sensitive Data Safety_

- [x] 25. Implement `tkr observability check`
  - Add a command group under `apps/tkr`:
    - `tkr observability check`
    - optional flags for platform, namespace/deployment, timeout, and output format.
  - The command MUST verify:
    - process scrape target discovery;
    - sample ingestion into Mimir or configured metrics backend;
    - Loki log ingestion;
    - dashboard provisioning;
    - alert rule loading;
    - optional trace backend reachability if trace export is enabled.
  - The command MUST emit a synthetic metric and structured log line and verify both are queryable where supported by the platform.
  - The command MUST return non-zero if any critical telemetry path is broken.
  - The command MUST print actionable remediation hints, including which scrape target, datasource, dashboard, or rule group failed.
  - _Requirements: Observability Smoke Tests, Platform Integration, Infrastructure Service Health Metrics_

  - [x]* 25.1 Smoke-test command tests
    - Unit-test command parsing and config loading.
    - Use fake clients for Mimir, Loki, Grafana, and Alloy where practical.
    - Test failure when metrics ingestion is missing.
    - Test failure when logs are not queryable.
    - Test failure when dashboards are not provisioned.
    - Test failure when alert rules are not loaded.
    - Test success when all fake checks pass.
    - _Requirements: Observability Smoke Tests_

- [x] 26. Add documentation and implementation guidance for operators and future agents
  - Add or update documentation covering:
    - observability architecture;
    - process endpoints;
    - metric manifest conventions;
    - safe label/cardinality rules;
    - structured logging fields;
    - trace export configuration;
    - Compose observability deployment;
    - ECS observability deployment;
    - `tkr observability check` usage;
    - dashboard catalogue;
    - alert catalogue and runbook index.
  - Add a short implementer note stating:
    - no workflow/run/request/trace identifiers in metric labels;
    - use manual spans on hot/cancellable paths;
    - add every new metric to a manifest;
    - add or update dashboard/alert validation tests when adding dashboards/alerts.
  - _Requirements: Alert Runbooks, Metric Label Cardinality Governance, Platform Integration_

- [x] 27. Phase 1 full workspace verification checkpoint
  - Run `cargo +nightly fmt --all --check`.
  - Run `cargo lint`.
  - Run `cargo test-lint`.
  - Run `cargo test --workspace`.
  - Run `cargo doc --workspace --no-deps`.
  - Run all metric manifest validation tests.
  - Run all dashboard validation tests.
  - Run all alert and runbook validation tests.
  - Run Compose platform generation tests.
  - Run ECS platform generation tests that do not require AWS credentials.
  - Run `tkr observability check` against fake/local test backends where available.
  - Verify no generated logs, configs, dashboards, or test snapshots expose secrets.
  - Mark the Phase 1 implementation complete only when all required Phase 1 tests pass or any platform-dependent tests are clearly documented as requiring external services.
  - _Requirements: All Phase 1 requirements; OTLP Metrics Push Export remains Phase 2_

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1", "1.1", "1.2", "1.3"] },
    { "id": 1, "tasks": ["2", "2.1"] },
    { "id": 2, "tasks": ["3", "3.1", "3.2", "4", "4.1"] },
    { "id": 3, "tasks": ["5", "5.1", "7", "7.1", "7.2", "8", "8.1", "9", "9.1", "9.2", "9.3", "9.4", "11", "11.1"] },
    { "id": 4, "tasks": ["6", "6.1"] },
    { "id": 5, "tasks": ["12", "12.1", "13", "13.1", "14", "14.1", "15", "15.1", "16", "16.1", "17", "17.1", "18", "18.1"] },
    { "id": 6, "tasks": ["19", "19.1", "22", "22.1"] },
    { "id": 7, "tasks": ["20", "20.1", "21", "21.1"] },
    { "id": 8, "tasks": ["23", "23.1", "24", "24.1"] },
    { "id": 9, "tasks": ["25", "25.1", "26"] },
    { "id": 10, "tasks": ["27"] },
    { "id": 11, "tasks": ["10", "10.1"] }
  ]
}
```

## Notes

- Tasks 1, 1.1, 1.2, and 1.3 intentionally establish validation before implementation. They must pass at every commit boundary by validating the current metric names, labels, and artifacts as-is. Tests for newly introduced metrics belong in the same task that adds the metric.
- Tasks 3 and 4 must land before broad subsystem instrumentation, because they define the manifest and label safety model used by every later metric.
- Tasks 5, 7, 8, 9, and 11 can be developed in parallel after task 2, but they must converge on the same `install_observability` API.
- Task 10 is Phase 2 OTLP metrics push work and is not a prerequisite for the Phase 1 verification checkpoint.
- Task 6 depends on task 2 and should preserve each process's existing startup semantics.
- Tasks 12 through 18 are subsystem instrumentation tasks. They can proceed in parallel after manifest/label governance is available.
- Tasks 20 and 21 should not be finalised until the metrics in tasks 12 through 19 are declared, because dashboard and alert queries must reference real manifest entries.
- Tasks 23 and 24 depend on dashboard/alert artifacts and the process endpoint contract.
- Task 25 depends on platform integration because the smoke test must validate the real generated telemetry path.
- The `*` suffix on task numbers indicates test tasks. These are required, not optional.
- If the implementation discovers that an exact metric name conflicts with the completed `dsql-observability-metrics` spec, the DSQL spec remains authoritative for DSQL metric names. Preserve compatibility and update this task file only if the requirements/design are updated as well.
- Do not introduce high-cardinality metric labels while implementing any task. Workflow IDs, run IDs, request IDs, trace IDs, raw SQL, raw errors, node endpoints, and ECS task ARNs belong in logs/spans, not metric labels.
- Do not use broad `#[instrument]` on hot or cancellable async storage/lane paths. Use explicit `tracing::span!` around stable boundaries.
- Ask the user if a task would require changing the public deployment model, adding a new external backend as mandatory, or weakening the private-only ECS observability posture.
