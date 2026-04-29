# Requirements Document: Configuration Foundation

## Introduction

Tokeira's `tokeirad` server binary has no unified configuration. The observability module reads 8 environment variables (`TOKEIRA_METRICS_ENABLED`, `TOKEIRA_METRICS_ADDR`, `TOKEIRA_OTLP_ENABLED`, `TOKEIRA_OTLP_ENDPOINT`, `TOKEIRA_OTLP_PROTOCOL`, `TOKEIRA_TRACE_SAMPLE_RATE`, `TOKEIRA_LOG_FORMAT`, `RUST_LOG`). The gRPC address is read from `TOKEIRA_GRPC_ADDR` in `main.rs`. Runtime subsystems (`LaneConfig`, `TimerScannerConfig`, `BacklogConfig`, `ActivityTimeoutScannerConfig`, `NexusTimeoutScannerConfig`, `WorkflowTimeoutScannerConfig`) use hardcoded `Default` implementations passed as constructor parameters. The `AppConfig` struct in `config.rs` is empty.

This spec delivers a TOML-based configuration file for `tokeirad`, replacing all env var reads and hardcoded defaults with a single typed config struct. The scope is strictly the server binary's runtime configuration — no deployment orchestration, no docker-compose generation, no profile merge, no CLI tooling beyond `--config` and `--dump-config` on `tokeirad` itself.

The implementation is organized into 3 phases:

- Phase 1: TOML config file format + `--config` CLI arg + loading + validation + defaults + `--dump-config`
- Phase 2: Config propagation to subsystems (replace env vars in observability, replace env var for gRPC addr, consolidate runtime config structs)
- Phase 3: Effective config endpoint (`GET /config`) with redaction

### What This Spec Covers

1. A `--config <path>` CLI argument on `tokeirad` (the tkr-cli spec depends on this for `deploy apply`)
2. A `TokeiraConfig` struct loaded from TOML, replacing all env var reads
3. Sensible defaults so zero-config startup still works
4. Config validation at startup (all errors collected)
5. `--dump-config` flag to print resolved config and exit
6. `GET /config` endpoint on the observability HTTP server for effective config retrieval
7. Migration of `ObservabilityConfig::from_env()` to TOML
8. Migration of `grpc_addr_from_env()` to TOML
9. Consolidation of runtime config structs into a `RuntimeConfig` aggregate

### What This Spec Does NOT Cover

- Deployment orchestration (owned by orchestrator-framework spec)
- Docker-compose generation (owned by tkr-cli spec)
- Profile merge / `--profile` flag (owned by tkr-cli spec)
- Dev profile files (owned by tkr-cli spec)
- Bootstrap scripts (owned by tkr-cli spec)
- `[deploy]` or `[observability_stack]` sections (owned by tkr-cli / platform crates)
- Security section (no TLS, no OIDC, no cert paths, no IAM roles — not implemented)
- `[capacity.hosts]` section (host scaling is a deployment concern)
- Architecture doc updates (deferred)
- Auto-tune control loops (covered by 065-runtime-auto-tune)
- Dynamic config hot-reload (deferred — restart is acceptable for MVP)

## Glossary

- **Platform_Config**: The TOML configuration file loaded at `tokeirad` startup. Located at a path specified by `--config` or the `TOKEIRA_CONFIG` environment variable.
- **Config_Loader**: The module responsible for reading the Platform_Config file, parsing TOML, applying defaults, validating constraints, and producing a typed `TokeiraConfig` struct.
- **TokeiraConfig**: The top-level Rust struct representing the fully resolved, validated configuration. Composed of sub-structs for infrastructure, policy, capacity, and emergency overrides.
- **Infrastructure_Config**: Configuration for environment facts: cluster name, region, DSQL endpoint, network addresses, observability settings.
- **Policy_Config**: Configuration for behavioral rules: retention, namespace creation policy, quotas.
- **Capacity_Config**: Configuration for resource envelopes and performance targets. Input to auto-tune.
- **Emergency_Config**: Configuration for break-glass overrides. Incident-response only.
- **RuntimeConfig**: An aggregate struct that holds `LaneConfig`, `TimerScannerConfig`, `BacklogConfig`, `ActivityTimeoutScannerConfig`, `NexusTimeoutScannerConfig`, and `WorkflowTimeoutScannerConfig`. Replaces the individual constructor parameters on `TokeiraRuntime`.

## Requirements

---

## Phase 1: TOML Config File, CLI Arguments, Loading, Validation, and Defaults

### Requirement 1.1: CLI Arguments for Config Loading

**User Story:** As a Tokeira operator, I want `tokeirad` to accept a `--config` argument, so that I can point it at a TOML config file.

#### Acceptance Criteria

1. THE `tokeirad` binary SHALL use `clap` for CLI argument parsing.
2. THE `tokeirad` binary SHALL accept an optional `--config <path>` argument specifying the path to a TOML configuration file.
3. WHEN `--config` is not provided, THE `tokeirad` binary SHALL check the `TOKEIRA_CONFIG` environment variable for a config file path, with `--config` taking precedence.
4. WHEN neither `--config` nor `TOKEIRA_CONFIG` is provided, THE `tokeirad` binary SHALL start with all-default configuration, enabling zero-config startup.
5. THE `tokeirad` binary SHALL accept a `--dump-config` flag that loads and validates the configuration, prints the resolved `TokeiraConfig` as TOML to stdout, and exits without starting the server.
6. WHEN the configuration contains validation errors, THE `--dump-config` flag SHALL print the errors to stderr and exit with a non-zero status code.

### Requirement 1.2: TOML Config File Format

**User Story:** As a Tokeira operator, I want a single TOML file that captures all server runtime intent, so that I can configure `tokeirad` from one readable file.

#### Acceptance Criteria

1. THE Platform_Config SHALL use TOML as the configuration file format.
2. THE Platform_Config SHALL contain four top-level sections corresponding to the configuration classes: `[infrastructure]`, `[policy]`, `[capacity]`, and `[emergency]`.
3. THE Platform_Config SHALL support nested tables within each section for logical grouping (e.g., `[infrastructure.dsql]`, `[infrastructure.network]`, `[infrastructure.observability]`, `[capacity.performance]`, `[capacity.dsql]`, `[policy.quotas]`).
4. THE Config_Loader SHALL use `serde(deny_unknown_fields)` for strict deserialization, so that typos in field names produce errors rather than silent misconfiguration.

### Requirement 1.3: Infrastructure Configuration

**User Story:** As a Tokeira operator, I want to declare where my server runs and how it connects, so that the system knows its environment facts.

#### Acceptance Criteria

1. THE Infrastructure_Config SHALL include fields for: `cluster_name` (string, default `"tokeira-local"`) and `region` (string, default `"us-east-1"`).
2. THE Infrastructure_Config SHALL include a `[infrastructure.dsql]` section with: `endpoint` (optional string, default `None`). This field is configuration metadata consumed by deployment tooling (e.g., tkr-cli writes it back via `collect_writeback`). Storage backend selection (in-memory vs DSQL) is wired by the dsql-storage-implementation spec, not this spec.
3. THE Infrastructure_Config SHALL include a `[infrastructure.network]` section with: `grpc_addr` (socket address, default `[::1]:7233`) and `metrics_addr` (socket address, default `0.0.0.0:9090`).
4. THE Infrastructure_Config SHALL include a `[infrastructure.observability]` section with fields for: `metrics_enabled` (boolean, default `true`), `otlp_enabled` (boolean, default `false`), `otlp_endpoint` (string, default `"http://localhost:4317"`), `otlp_protocol` (enum: `grpc` | `http`, default `grpc`), `trace_sample_rate` (f64, default `1.0`), `log_format` (enum: `text` | `json`, default `text`), and `log_filter` (string, default `"info"`).
5. THE observability fields SHALL map one-to-one with the current `ObservabilityConfig` struct fields, preserving identical default values.

### Requirement 1.4: Policy Configuration

**User Story:** As a Tokeira operator, I want to declare behavioral rules for the server, so that retention and quotas are explicit.

#### Acceptance Criteria

1. THE Policy_Config SHALL include fields for: `default_retention_days` (u32, default `30`) and `namespace_creation` (enum: `open` | `controlled`, default `open`).
2. THE Policy_Config SHALL include a `[policy.quotas]` section with: `max_workflow_timeout_seconds` (u64, default `315360000` — 10 years) and `max_signal_payload_bytes` (u32, default `4194304` — 4 MiB).

### Requirement 1.5: Capacity Configuration

**User Story:** As a Tokeira operator, I want to declare performance targets and resource budgets, so that the system knows its operating envelope.

#### Acceptance Criteria

1. THE Capacity_Config SHALL include a `[capacity.performance]` section with: `target_workflow_starts_per_second` (u32, default `1000`) and `target_p99_wft_latency_ms` (u32, default `50`).
2. THE Capacity_Config SHALL include a `[capacity.dsql]` section with: `max_connections` (u32, default `10000`), `connection_rate_per_second` (u32, default `100`), and `burst_capacity` (u32, default `1000`).

### Requirement 1.6: Emergency Override Configuration

**User Story:** As a Tokeira operator, I want break-glass overrides for incident response, so that I can temporarily force specific system behaviors.

#### Acceptance Criteria

1. THE Emergency_Config SHALL include optional fields for: `disable_stickiness` (boolean, default `false`), `freeze_projection` (boolean, default `false`), and `cap_poll_admission` (optional u32, default `None`).
2. WHEN any Emergency_Config field is set to a non-default value, THE Config_Loader SHALL log a warning at startup indicating that emergency overrides are active.

### Requirement 1.7: Config Loading and Parsing

**User Story:** As a Tokeira developer, I want a config loader that reads TOML, applies defaults, and produces a typed struct, so that all subsystems receive validated configuration at startup.

#### Acceptance Criteria

1. THE Config_Loader SHALL parse the Platform_Config TOML file into the typed `TokeiraConfig` struct using the `toml` crate with `serde` deserialization.
2. THE Config_Loader SHALL apply default values for every field, so that an empty TOML file produces a valid configuration.
3. WHEN the TOML file contains unknown keys, THE Config_Loader SHALL reject the file with an error listing the unknown keys.
4. WHEN the TOML file contains a value of the wrong type for a field, THE Config_Loader SHALL return the `toml` crate's diagnostic error, which includes the field path and error description.
5. THE Config_Loader SHALL use `thiserror` for config error types.
6. FOR ALL valid `TokeiraConfig` values, serializing to TOML then deserializing SHALL produce an equivalent struct (round-trip property).

### Requirement 1.8: Config Validation

**User Story:** As a Tokeira operator, I want configuration validated at startup with all errors reported, so that misconfigurations are caught before the server begins processing.

#### Acceptance Criteria

1. THE Config_Loader SHALL validate all cross-field constraints after parsing.
2. WHEN validation fails, THE Config_Loader SHALL collect and return all validation errors, not just the first. Each error SHALL identify the field path and the violated constraint.
3. THE Config_Loader SHALL validate that `default_retention_days` is between 1 and 36500.
4. THE Config_Loader SHALL validate that `target_workflow_starts_per_second` is a positive integer.
5. THE Config_Loader SHALL validate that `target_p99_wft_latency_ms` is a positive integer.
6. THE Config_Loader SHALL validate that `trace_sample_rate` is between 0.0 and 1.0 inclusive.
7. THE Config_Loader SHALL validate that `grpc_addr` and `metrics_addr` are parseable socket addresses.

### Requirement 1.9: Sensible Defaults for Zero-Config Startup

**User Story:** As a Tokeira developer, I want every field to have a sensible default, so that `tokeirad` starts with zero configuration for local development.

#### Acceptance Criteria

1. THE `TokeiraConfig` SHALL define defaults for every field such that `TokeiraConfig::default()` produces a valid, usable configuration for single-node local development.
2. THE default configuration SHALL use `"tokeira-local"` as cluster name, `[::1]:7233` as gRPC address, `0.0.0.0:9090` as metrics address, and `"us-east-1"` as region.
3. THE default configuration SHALL produce the same runtime behavior as the current hardcoded defaults in `main.rs` and `ObservabilityConfig::from_env()`, ensuring backward compatibility.
4. THE default configuration SHALL set `namespace_creation = open` and `dsql.endpoint = None`.

---

## Phase 2: Config Propagation to Subsystems

### Requirement 2.1: Observability Config Migration

**User Story:** As a Tokeira developer, I want the observability module to read configuration from the TOML config instead of environment variables, so that all configuration comes from a single source.

#### Acceptance Criteria

1. WHEN the Platform_Config is loaded, THE `ObservabilityConfig` struct SHALL be constructed from the `[infrastructure.observability]` section instead of reading individual environment variables.
2. THE `ObservabilityConfig::from_env()` method SHALL be removed after migration is complete.
3. THE observability module SHALL produce identical behavior when configured via TOML as when configured via the equivalent environment variables.
4. THE `metrics_addr` field SHALL move from `ObservabilityConfig` to `[infrastructure.network]` in the TOML, since it is a network address. The observability module SHALL receive it from the loaded config.

### Requirement 2.2: gRPC Address Migration

**User Story:** As a Tokeira developer, I want the gRPC address to come from the TOML config instead of `TOKEIRA_GRPC_ADDR`, so that network configuration is centralized.

#### Acceptance Criteria

1. THE `grpc_addr` SHALL be read from `[infrastructure.network.grpc_addr]` in the loaded config.
2. THE `grpc_addr_from_env()` function in `main.rs` SHALL be removed after migration is complete.

### Requirement 2.3: Runtime Config Consolidation

**User Story:** As a Tokeira developer, I want a single `RuntimeConfig` struct that aggregates all runtime subsystem configs, so that the runtime constructor receives one struct instead of six individual parameters.

#### Acceptance Criteria

1. THE `RuntimeConfig::default()` implementation SHALL aggregate: `lane_count` (usize, default `4`), `LaneConfig` (default: `max_occ_retries = 5`, `max_drain_per_activation = 16`), `TimerScannerConfig` (default: `scan_interval = 200ms`, `max_timers_per_scan = 100`), `BacklogConfig`, `ActivityTimeoutScannerConfig`, `NexusTimeoutScannerConfig`, and `WorkflowTimeoutScannerConfig`. These fields are intentionally not exposed in the TOML schema or constructed by the Config_Loader — they are mechanical settings owned by auto-tune.
2. THE `TokeiraRuntime` constructor SHALL accept a `RuntimeConfig` struct instead of individual parameters.
3. THE default `RuntimeConfig` SHALL produce the same behavior as the current `Default` implementations for all constituent config structs.
4. THE existing config structs (`LaneConfig`, `TimerScannerConfig`, etc.) SHALL remain unchanged in `tokeira-runtime` — `RuntimeConfig` aggregates them without modifying their definitions.

### Requirement 2.4: Main Function Refactor

**User Story:** As a Tokeira developer, I want `main.rs` to load config once and pass slices to each subsystem, so that the startup path is clean and testable.

#### Acceptance Criteria

1. THE `main()` function SHALL load the `TokeiraConfig` as its first action (after CLI arg parsing).
2. THE `main()` function SHALL construct `ObservabilityConfig` from the loaded config's `[infrastructure.observability]` section.
3. THE `main()` function SHALL read `grpc_addr` from the loaded config's `[infrastructure.network]` section.
4. THE `main()` function SHALL construct `RuntimeConfig::default()` and pass it to `TokeiraRuntime`. Runtime fields are not derived from the TOML config in this MVP.
5. THE kernel SHALL remain pure — no config dependency SHALL be added to `tokeira-kernel`.

---

## Phase 3: Effective Config Endpoint

### Requirement 3.1: Effective Config HTTP Endpoint

**User Story:** As a Tokeira operator, I want to query the running server's effective configuration, so that I can verify what the system is actually using.

#### Acceptance Criteria

1. THE observability HTTP server SHALL expose a `GET /config` endpoint on the same listener as `/metrics`.
2. THE observability HTTP listener SHALL always run, regardless of the `metrics_enabled` setting. When `metrics_enabled = false`, the `/metrics` endpoint returns an empty response, but `/config` and `/loglevel` remain available.
3. THE `GET /config` endpoint SHALL return a JSON representation of the current `TokeiraConfig` with all defaults resolved.
4. THE `GET /config` endpoint SHALL redact sensitive fields: any field whose key contains `endpoint` or `arn` SHALL have its value replaced with `"[redacted]"` in the response. Listener addresses (`grpc_addr`, `metrics_addr`) are NOT redacted — they are operationally useful, not secrets.
5. WHEN emergency overrides are active, THE `GET /config` response SHALL include a `_warnings` array listing each active override.
