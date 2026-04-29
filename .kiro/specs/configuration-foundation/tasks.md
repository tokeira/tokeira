# Tasks: Configuration Foundation

## Phase 1: TOML Config File, CLI Arguments, Loading, Validation, and Defaults

- [x] 1. Define TokeiraConfig struct hierarchy in `apps/tokeirad/src/config.rs`
  - [x] 1.1 Replace empty `AppConfig` with `TokeiraConfig` and all sub-structs (`InfrastructureConfig`, `DsqlInfraConfig`, `NetworkConfig`, `ObservabilityTomlConfig`, `OtlpProtocol`, `LogFormatConfig`, `PolicyConfig`, `NamespaceCreationPolicy`, `QuotasConfig`, `CapacityConfig`, `PerformanceConfig`, `DsqlCapacityConfig`, `EmergencyConfig`) with `serde(deny_unknown_fields)` on all structs
  - [x] 1.2 Implement `Default` for all config structs with values matching the design defaults table
  - [x] 1.3 Implement serde default functions (`default_cluster_name`, `default_region`, `default_grpc_addr`, `default_metrics_addr`, `default_true`, `default_otlp_endpoint`, `default_otlp_protocol`, `default_sample_rate`, `default_log_format`, `default_log_filter`, `default_retention_days`, `default_max_workflow_timeout`, `default_max_signal_payload`, `default_target_wf_starts`, `default_target_p99`, `default_max_connections`, `default_conn_rate`, `default_burst`)
  - [x] 1.4 Derive `PartialEq` on all config structs (needed for round-trip property test)
  - [x] 1.5 Add `toml`, `serde`, `clap`, and `thiserror` dependencies to `apps/tokeirad/Cargo.toml`
- [x] 2. Implement config error types and validation
  - [x] 2.1 Define `ConfigError` enum with `Io`, `Parse`, and `Validation` variants using `thiserror`
  - [x] 2.2 Define `ValidationError` enum with `Field { field, message }` variant
  - [x] 2.3 Implement `TokeiraConfig::validate()` that collects all errors: retention days bounds [1, 36500], positive `target_workflow_starts_per_second`, positive `target_p99_wft_latency_ms`, trace_sample_rate bounds [0.0, 1.0], grpc_addr and metrics_addr socket address parseability
  - [x] 2.4 Implement `TokeiraConfig::emergency_warnings()` returning a `Vec<String>` for each non-default emergency field
- [x] 3. Implement config loading
  - [x] 3.1 Implement `TokeiraConfig::load(path: &Path)` that reads file, parses TOML, validates, returns `Result<Self, ConfigError>`
  - [x] 3.2 Implement `TokeiraConfig::resolve(config_path: Option<&Path>)` with precedence: `--config` > `TOKEIRA_CONFIG` env var > `TokeiraConfig::default()`
  - [x] 3.3 Implement `TokeiraConfig::to_toml()` for `--dump-config` output
- [x] 4. Add clap CLI argument parsing to tokeirad
  - [x] 4.1 Define `Cli` struct with `--config <path>` (optional) and `--dump-config` flag using `clap::Parser`
  - [x] 4.2 Wire CLI parsing into `main()` as the first action before any config loading
  - [x] 4.3 Implement `--dump-config` behavior: load config, validate, print TOML to stdout, exit 0; on validation error, print errors to stderr, exit 1
- [x] 5. Write property-based tests for Phase 1
  - [x] 5.1 [PBT] Property 1: TOML round-trip — generate arbitrary `TokeiraConfig`, serialize to TOML, deserialize back, assert equality (proptest, 100 iterations)
  - [x] 5.2 [PBT] Property 2: Unknown fields rejection — generate valid TOML with injected unknown key, assert deserialization fails (proptest, 100 iterations)
  - [x] 5.3 [PBT] Property 3: Retention days bounds — generate random u32, set as `default_retention_days`, validate, assert error iff outside [1, 36500] (proptest, 100 iterations)
  - [x] 5.4 [PBT] Property 4: Positive integer validation — generate random u32 for `target_workflow_starts_per_second` and `target_p99_wft_latency_ms`, validate, assert error iff 0 (proptest, 100 iterations)
  - [x] 5.5 [PBT] Property 5: Trace sample rate bounds — generate random f64, set as `trace_sample_rate`, validate, assert error iff outside [0.0, 1.0] (proptest, 100 iterations)
  - [x] 5.6 [PBT] Property 6: Validation error collection — generate configs with 2+ known violations, validate, assert error count matches violation count (proptest, 100 iterations)
- [x] 6. Write unit tests for Phase 1
  - [x] 6.1 Test that empty TOML string deserializes to `TokeiraConfig::default()` and passes validation
  - [x] 6.2 Test that default config values match current env var defaults (grpc_addr, metrics_addr, observability fields)
  - [x] 6.3 Test that wrong type in TOML produces descriptive parse error
  - [x] 6.4 Test that `--dump-config` with valid config prints TOML to stdout
  - [x] 6.5 Test config source precedence: `--config` > `TOKEIRA_CONFIG` > defaults

## Phase 2: Config Propagation to Subsystems

- [x] 7. Define RuntimeConfig in tokeira-runtime
  - [x] 7.1 Add `RuntimeConfig` struct to `crates/tokeira-runtime/src/runtime.rs` aggregating `lane_count`, `LaneConfig`, `TimerScannerConfig`, `WorkflowTimeoutScannerConfig`, `BacklogConfig`, `ActivityTimeoutScannerConfig`, `NexusTimeoutScannerConfig` with `Default` impl matching current individual defaults. These fields are intentionally not exposed in TOML — they are mechanical settings owned by auto-tune.
  - [x] 7.2 Add a new `TokeiraRuntime` constructor that accepts `RuntimeConfig` instead of individual parameters
  - [x] 7.3 Update `lib.rs` to export `RuntimeConfig`
- [x] 8. Migrate observability config from env vars to TOML
  - [x] 8.1 Add a method or `From` impl in tokeirad to construct `ObservabilityConfig` from `TokeiraConfig`'s `infrastructure.observability` section + `infrastructure.network.metrics_addr`
  - [x] 8.2 Remove `ObservabilityConfig::from_env()` method
  - [x] 8.3 Remove env var reads from observability tests (update tests to construct config directly)
- [x] 9. Migrate gRPC address from env var to TOML
  - [x] 9.1 Read `grpc_addr` from loaded `TokeiraConfig`'s `infrastructure.network.grpc_addr` in `main()`
  - [x] 9.2 Remove `grpc_addr_from_env()` function from `main.rs`
- [x] 10. Refactor main() to use config-driven startup
  - [x] 10.1 Update `main()` to: parse CLI args → resolve config → log emergency warnings → construct `ObservabilityConfig` from loaded config → construct `RuntimeConfig::default()` → pass `RuntimeConfig` to `TokeiraRuntime` constructor → read `grpc_addr` from loaded config
  - [x] 10.2 Verify `tokeira-kernel` Cargo.toml has no config-related dependencies
- [x] 11. Write tests for Phase 2
  - [x] 11.1 Test that `RuntimeConfig::default()` field values match individual struct `Default` implementations
  - [x] 11.2 Test that `ObservabilityConfig` constructed from default `TokeiraConfig` matches the old `from_env()` defaults
  - [x] 11.3 Test that `grpc_addr` from default `TokeiraConfig` matches old `grpc_addr_from_env()` default

## Phase 3: Effective Config Endpoint

- [x] 12. Implement GET /config endpoint
  - [x] 12.1 Add `Arc<TokeiraConfig>` to `ObservabilityServerState`
  - [x] 12.2 Ensure the observability HTTP listener always runs, even when `metrics_enabled = false`. When metrics are disabled, `/metrics` returns an empty response but `/config` and `/loglevel` remain available.
  - [x] 12.3 Add `GET /config` route to `handle_observability()` that calls `to_redacted_json()` and returns JSON with `application/json` content type
  - [x] 12.4 Implement `to_redacted_json()` on `TokeiraConfig`: serialize to JSON, redact fields whose key contains `endpoint` or `arn` (NOT `addr` — listener addresses are operationally useful), append `_warnings` array for active emergency overrides
  - [x] 12.5 Wire `Arc<TokeiraConfig>` into `spawn_observability_server()` from `main()`
- [x] 13. Write property-based tests for Phase 3
  - [x] 13.1 [PBT] Property 7: Sensitive field redaction — generate config with random sensitive field values, call `to_redacted_json()`, assert fields with `endpoint` or `arn` in key contain `"[redacted]"`, assert `grpc_addr` and `metrics_addr` are NOT redacted (proptest, 100 iterations)
  - [x] 13.2 [PBT] Property 8: Emergency warnings — generate random `EmergencyConfig`, call `to_redacted_json()`, assert `_warnings` presence matches override state (proptest, 100 iterations)
- [x] 14. Write unit tests for Phase 3
  - [x] 14.1 Test that `GET /config` returns 200 with valid JSON
  - [x] 14.2 Test that `GET /config` response redacts endpoint/arn values but preserves grpc_addr and metrics_addr
  - [x] 14.3 Test that `GET /config` with active emergency overrides includes `_warnings` array
  - [x] 14.4 Test that `GET /config` with default emergency config has no `_warnings` key
