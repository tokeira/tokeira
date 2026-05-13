# Implementation Plan: Compose Observability Config Provisioning

## Overview

Implement observability configuration files as a first-class IaC resource (`ObservabilityConfigFilesResource`) in the compose platform. The resource owns all file I/O through its lifecycle methods (create/update/delete/describe/diff). `ConfigGenerator` only renders — it never writes. The module constructor remains infallible. Compose service resources depend on the config resource via IaC dependencies (not Docker Compose `depends_on`).

## Tasks

- [ ] 1. Add askama dependency and create template files
  - [ ] 1.1 Add `askama` dependency to `platforms/compose/Cargo.toml`
    - Add `askama = "0.12"` (or latest 0.12.x) to `[dependencies]`
    - Add `thiserror` if not already present
    - _Requirements: 11.1_

  - [ ] 1.2 Create Alloy River template at `platforms/compose/templates/alloy.alloy`
    - Template defines `prometheus.scrape`, `prometheus.remote_write`, `discovery.docker`, `discovery.relabel`, `loki.source.docker`, `loki.write` components
    - Uses template variables: `metrics_target_host`, `metrics_target_port`, `mimir_remote_write_url`, `loki_push_url`
    - _Requirements: 1.2, 1.3, 1.4, 1.5, 1.6_

  - [ ] 1.3 Create Mimir YAML template at `platforms/compose/templates/mimir.yaml`
    - Template configures single-binary mode (`target: all`), filesystem storage at `/data/mimir`, HTTP port via `http_port` variable
    - _Requirements: 2.2, 2.3, 2.4_

  - [ ] 1.4 Create Loki YAML template at `platforms/compose/templates/loki.yaml`
    - Template configures single-binary mode, filesystem storage at `/loki`, retention via `retention_hours` variable, HTTP port via `http_port` variable
    - _Requirements: 3.2, 3.3, 3.4, 3.5_

  - [ ] 1.5 Create Grafana datasources template at `platforms/compose/templates/grafana-datasources.yaml`
    - Template defines Mimir (Prometheus type, default) and Loki datasources using `mimir_url` and `loki_url` variables
    - _Requirements: 4.2, 4.3_

  - [ ] 1.6 Create Grafana dashboard provider template at `platforms/compose/templates/grafana-dashboards.yaml`
    - Template defines file-based provider with `dashboards_path` variable, `disableDeletion: true`, `updateIntervalSeconds: 30`
    - _Requirements: 5.2, 5.3_

- [ ] 2. Implement ObservabilityParams, ConfigGenerator, and template structs
  - [ ] 2.1 Create `platforms/compose/src/observability_config.rs` with template structs and params
    - Define `AlloyConfigTemplate`, `MimirConfigTemplate`, `LokiConfigTemplate`, `GrafanaDatasourcesTemplate`, `GrafanaDashboardProviderTemplate` — each deriving `askama::Template` with `#[template(path = "...", escape = "none")]`
    - Define `ObservabilityParams` with `from_config(&ComposeConfig)` — uses `"tokeirad"` as `metrics_target_host` (compose DNS), derives ports and URLs from config
    - Define `ConfigGenError` enum with `InvalidParameter`, `RenderFailed`, `WriteFailed` variants
    - Define `RenderedConfigFile { relative_path: PathBuf, contents: String }`
    - _Requirements: 11.1, 11.2, 11.3_

  - [ ] 2.2 Implement `ConfigGenerator` with `validate()` and `render_all()`
    - `ConfigGenerator::new(deployment_dir)` stores the deployment dir
    - `validate(&self, params)` checks non-empty URLs, non-zero ports — returns `ConfigGenError::InvalidParameter` on failure
    - `render_all(&self, params)` calls `validate()` then renders all 5 templates plus 3 dashboard JSON files, returning `Vec<RenderedConfigFile>` with relative paths under `config/`
    - Individual render methods: `render_alloy`, `render_mimir`, `render_loki`, `render_grafana_datasources`, `render_grafana_dashboard_provider`
    - Dashboard methods: `grpc_edge_dashboard`, `broker_runtime_dashboard`, `storage_projection_dashboard` — return static JSON via `include_str!`
    - ConfigGenerator NEVER writes to disk — only returns rendered content
    - _Requirements: 1.6, 2.4, 3.4, 3.5, 4.2, 4.3, 11.2, 11.3_

  - [ ]* 2.3 Write property test: template parameter injection (Property 1)
    - **Property 1: Template parameter injection**
    - Generate random valid `ObservabilityParams` and verify rendered output contains all parameter values
    - **Validates: Requirements 1.6, 2.4, 3.4, 3.5, 4.2, 4.3**

  - [ ]* 2.4 Write property test: invalid parameters are rejected (Property 3)
    - **Property 3: Invalid parameters are rejected**
    - Generate `ObservabilityParams` with at least one empty URL or zero port, verify `ConfigGenError::InvalidParameter` is returned
    - **Validates: Requirements 11.3**

  - [ ]* 2.5 Write property test: config file output completeness (Property 5)
    - **Property 5: Config file output completeness**
    - For any valid `ObservabilityParams`, verify `render_all()` produces exactly 8 files with the expected relative paths
    - **Validates: Requirements 1.1, 2.1, 3.1, 4.1, 5.1, 6.1, 7.1, 8.1**

- [ ] 3. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 4. Create dashboard JSON files
  - [ ] 4.1 Create gRPC/Edge Health dashboard JSON
    - Create `platforms/compose/dashboards/grpc-edge-health.json`
    - Include panels: `tokeira_build_info` stat, request rate, error rate, error ratio, latency p50/p95/p99, active requests
    - Use Mimir datasource UID `mimir`
    - _Requirements: 6.1, 6.2, 6.3, 13.1_

  - [ ] 4.2 Create Broker/Runtime Health dashboard JSON
    - Create `platforms/compose/dashboards/broker-runtime-health.json`
    - Include panels: `tokeira_build_info` stat, publish rate, sync/non-sync match rates, poll timeout rate, queue depth, lane submit latency p50/p95/p99, scanner tick/dispatched rates, OCC retry rate
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 13.1_

  - [ ] 4.3 Create Storage/Projection Health dashboard JSON
    - Create `platforms/compose/dashboards/storage-projection-health.json`
    - Include panels: `tokeira_build_info` stat, commit transition/load run/read history latency p50/p95/p99, storage operations by type, projection records processed, projection lag, sink write latency, sink error rate
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 13.1_

- [ ] 5. Implement ObservabilityConfigFilesResource (IaC Resource trait)
  - [ ] 5.1 Implement `ObservabilityConfigFilesResource` struct and `Resource` trait
    - Struct fields: `deployment_dir: PathBuf`, `params: ObservabilityParams`
    - `resource_type()` returns `ResourceType::new("observability_config_files")`
    - `resource_id()` returns `ResourceId("compose/observability-config-files".into())`
    - `dependencies()` returns empty vec (config resource has no IaC dependencies)
    - `module()` returns `"observability"`
    - `describe()` reads managed file paths, computes per-file SHA-256 checksums, stores in `ResourceState.properties`
    - `diff()` compares stored checksums against `desired_files()` output — reports Create if no state, Update if checksums differ, NoChange otherwise
    - `create()` calls `ensure_directories()` then writes each file from `desired_files()`
    - `update()` same as `create()` — idempotent overwrite
    - `delete()` removes only the files listed by `desired_files()`, then prunes empty generated directories without deleting unrelated files
    - `ensure_directories()` creates `config/`, `config/grafana/provisioning/datasources/`, `config/grafana/provisioning/dashboards/`, `config/grafana/dashboards/`
    - Errors propagated as `IacError::Other`
    - _Requirements: 12.1, 12.3, 12.4, 12.5, 12.6, 12.7, 12.8_

  - [ ]* 5.2 Write unit tests for ObservabilityConfigFilesResource lifecycle
    - Test `create()` writes all 8 expected files to a temp directory
    - Test `describe()` returns checksums for existing files, returns None when files missing
    - Test `diff()` reports NoChange when files match, Update when content differs
    - Test `delete()` removes managed files and empty dirs without touching unrelated files
    - _Requirements: 12.3, 12.4, 12.5, 12.7, 12.8_

  - [ ]* 5.3 Write property test: rendering failure halts execution (Property 4)
    - **Property 4: Rendering failure halts execution**
    - Simulate config resource error (e.g., read-only temp directory) and verify error propagation
    - **Validates: Requirements 12.6**

- [ ] 6. Update compose services with volume mounts and commands
  - [ ] 6.1 Update `compose_services()` in `platforms/compose/src/compose.rs`
    - Add config volume mount to mimir: `<deploy_dir>/config/mimir.yaml:/etc/mimir/mimir.yaml`
    - Add command to mimir: `["--config.file=/etc/mimir/mimir.yaml"]`
    - Add config volume mount to loki: `<deploy_dir>/config/loki.yaml:/etc/loki/loki.yaml`
    - Add command to loki: `["--config.file=/etc/loki/loki.yaml"]`
    - Add config volume mount to alloy: `<deploy_dir>/config/alloy.alloy:/etc/alloy/config.alloy`
    - Add command to alloy: `["run", "/etc/alloy/config.alloy"]`
    - Add provisioning and dashboards volume mounts to grafana: `<deploy_dir>/config/grafana/provisioning/:/etc/grafana/provisioning/` and `<deploy_dir>/config/grafana/dashboards/:/var/lib/grafana/dashboards/`
    - Change alloy metrics target from `host.docker.internal` to `tokeirad` (compose DNS)
    - Remove environment variables from alloy that are now handled by the config file (TOKEIRAD_METRICS_TARGET, MIMIR_REMOTE_WRITE_URL, LOKI_WRITE_URL)
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 10.1, 10.2, 10.3_

  - [ ]* 6.2 Write property test: volume mount path consistency (Property 2)
    - **Property 2: Volume mount path consistency**
    - For any valid deployment directory path, verify volume mounts reference paths that match `ObservabilityConfigFilesResource` output paths
    - **Validates: Requirements 9.1, 9.2, 9.3, 9.4**

- [ ] 7. Integrate config resource into ComposeModule
  - [ ] 7.1 Add `config_files` field to `ComposeModule` and update `observability()` constructor
    - Add `config_files: Option<ObservabilityConfigFilesResource>` field to `ComposeModule` struct
    - In `ComposeModule::observability()`: create `ObservabilityParams::from_config(config)`, construct `ObservabilityConfigFilesResource::new(config.deployment_dir.clone(), params)`, store in `config_files` field
    - In `ComposeModule::runtime()`: set `config_files: None`
    - Constructor remains infallible — no rendering or I/O happens here
    - _Requirements: 12.1_

  - [ ] 7.2 Update `Module::resources()` to return config resource FIRST
    - When `config_files` is `Some`, insert it as the first element in the returned `Vec<Box<dyn Resource>>`
    - Compose service resources follow after the config resource
    - _Requirements: 12.2, 12.5_

  - [ ] 7.3 Update `OwnedComposeResource` to add config resource IaC dependency
    - Add a `config_resource_id: Option<ResourceId>` field to `OwnedComposeResource`
    - In `dependencies()`: append `config_resource_id` to the list returned by `self.service.dependencies()` when present
    - This is an IaC dependency only — it does NOT add to Docker Compose `depends_on`
    - In `ComposeModule::resources()` for observability module: pass `Some(ResourceId("compose/observability-config-files".into()))` to each `OwnedComposeResource` wrapping mimir, loki, grafana, alloy
    - _Requirements: 12.2_

  - [ ]* 7.4 Write unit tests for module resource ordering and dependencies
    - Verify `resources()` returns config resource first (resource_id = `compose/observability-config-files`)
    - Verify mimir, loki, grafana, alloy resources each list `compose/observability-config-files` in their `dependencies()`
    - Verify runtime module resources do NOT have config resource dependency
    - _Requirements: 12.1, 12.2_

- [ ] 8. Register module in lib.rs and wire exports
  - [ ] 8.1 Update `platforms/compose/src/lib.rs` to declare `observability_config` module
    - Add `pub mod observability_config;` declaration
    - No signature changes to existing public API
    - Ensure `ComposeModule::observability()` still works from `infra_modules()`
    - _Requirements: 12.1_

- [ ] 9. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties from the design document
- Unit tests validate specific examples and edge cases
- The config resource is an IaC resource — NOT a side-effect of module construction
- `ConfigGenerator` only renders, never writes — file I/O is owned by the resource lifecycle
- `ComposeModule::observability()` remains infallible — no rendering or I/O during construction
- The config resource ID `compose/observability-config-files` is added to IaC dependencies of compose service resources, NOT to Docker Compose `depends_on`
- Alloy scrapes `tokeirad` by compose service name (compose DNS), NOT `host.docker.internal`

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2", "1.3", "1.4", "1.5", "1.6"] },
    { "id": 1, "tasks": ["2.1", "4.1", "4.2", "4.3"] },
    { "id": 2, "tasks": ["2.2"] },
    { "id": 3, "tasks": ["2.3", "2.4", "2.5"] },
    { "id": 4, "tasks": ["5.1"] },
    { "id": 5, "tasks": ["5.2", "5.3", "6.1"] },
    { "id": 6, "tasks": ["6.2", "7.1"] },
    { "id": 7, "tasks": ["7.2", "7.3"] },
    { "id": 8, "tasks": ["7.4", "8.1"] }
  ]
}
```
