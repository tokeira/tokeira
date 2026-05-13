# Design Document: Compose Observability Provisioning

## Overview

The observability config provisioning system models generated configuration files as an IaC resource owned by the compose observability module. Plans render desired contents and compare them with disk without writing. Apply writes `<deployment_dir>/config/` before the observability Docker Compose services are created or updated. Three Grafana dashboards provide focused views of tokeirad health.

```
┌─────────────────────────────────────────────────────────────────┐
│  Observability Module Plan/Apply                                │
│                                                                 │
│  1. Build desired resources without side effects                │
│  2. Render desired config contents                              │
│  3. Plan: compare disk checksums only                           │
│  4. Apply: write config files, then reconcile services          │
│  5. Destroy: remove owned generated files                       │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
         │                                    │
         ▼                                    ▼
┌─────────────────────┐          ┌──────────────────────────┐
│  Config Files       │          │  ComposeService resources│
│  (IaC resource)     │          │  (depend on config files)│
│                     │          │                          │
│  config/            │◄─────────│  mimir: --config.file=.. │
│    alloy.alloy      │  bind    │  loki:  --config.file=.. │
│    mimir.yaml       │  mount   │  alloy: run /etc/...     │
│    loki.yaml        │          │  grafana: provisioning/  │
│    grafana/         │          │                          │
│      provisioning/  │          └──────────────────────────┘
│      dashboards/    │
└─────────────────────┘
```

## Architecture

Config generation is an IaC local-file resource, not a side effect of module discovery. The flow is:

1. `ComposeModule::observability(config)` is called during IaC plan/apply and remains infallible.
2. The module returns an `ObservabilityConfigFilesResource` plus the observability `ComposeService` resources.
3. The config resource derives `ObservabilityParams` from `ComposeConfig` and uses `ConfigGenerator` to render desired file contents.
4. Plan/refresh compare desired checksums against files on disk without writing.
5. Apply writes the generated files before Docker resources because the compose services depend on the config resource.
6. Destroy removes only the managed generated files and empty generated directories.

Config generation is idempotent and state-tracked. Re-running apply overwrites managed files with the rendered desired content. The generated `config/` directory is co-located with the deployment's `docker-compose.yml` and `.tokeira-state/` directory. Unrelated files under `config/` are not owned by this resource.

### Data Flow

```
ComposeConfig
    │
    ▼
ObservabilityParams::from_config()
    │
    ├─► ObservabilityConfigFilesResource
    │
    ├─► ConfigGenerator::validate()     ── fail fast on invalid params
    │
    ├─► ConfigGenerator::render_all()   ── no filesystem writes
    │
    ├─► Plan/refresh checksum comparison
    │
    ├─► Apply: create directories
    │       config/
    │       config/grafana/provisioning/datasources/
    │       config/grafana/provisioning/dashboards/
    │       config/grafana/dashboards/
    │
    ├─► Apply: write rendered files
    │
    ▼
compose_services(config)
    │
    ▼
ComposeService resources with volumes + commands referencing generated paths
```

## Components and Interfaces

### 1. Template Structs (`platforms/compose/src/observability_config.rs`)

Each configuration file is backed by a typed Askama template struct. Template source files live in `platforms/compose/templates/`.

```rust
use askama::Template;

/// Alloy River configuration template.
#[derive(Template, Debug)]
#[template(path = "alloy.alloy", escape = "none")]
pub struct AlloyConfigTemplate {
    /// Hostname or Docker service name for tokeirad metrics scraping.
    pub metrics_target_host: String,
    /// Port where tokeirad exposes /metrics.
    pub metrics_target_port: u16,
    /// Mimir remote-write endpoint URL.
    pub mimir_remote_write_url: String,
    /// Loki push endpoint URL.
    pub loki_push_url: String,
}

/// Mimir YAML configuration template.
#[derive(Template, Debug)]
#[template(path = "mimir.yaml", escape = "none")]
pub struct MimirConfigTemplate {
    /// HTTP listen port for Mimir API.
    pub http_port: u16,
}

/// Loki YAML configuration template.
#[derive(Template, Debug)]
#[template(path = "loki.yaml", escape = "none")]
pub struct LokiConfigTemplate {
    /// HTTP listen port for Loki API.
    pub http_port: u16,
    /// Retention period in hours.
    pub retention_hours: u32,
}

/// Grafana datasource provisioning template.
#[derive(Template, Debug)]
#[template(path = "grafana-datasources.yaml", escape = "none")]
pub struct GrafanaDatasourcesTemplate {
    /// Mimir Prometheus-compatible query URL.
    pub mimir_url: String,
    /// Loki query URL.
    pub loki_url: String,
}

/// Grafana dashboard provider template.
#[derive(Template, Debug)]
#[template(path = "grafana-dashboards.yaml", escape = "none")]
pub struct GrafanaDashboardProviderTemplate {
    /// Path inside the Grafana container where dashboards are mounted.
    pub dashboards_path: String,
}
```

### 2. Config Generator (`platforms/compose/src/observability_config.rs`)

The `ConfigGenerator` struct owns validation and rendering. It does not write during module construction or planning; file I/O is owned by `ObservabilityConfigFilesResource` during resource create/update/delete.

```rust
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigGenError {
    #[error("invalid template parameter: {field} cannot be {reason}")]
    InvalidParameter { field: String, reason: String },
    #[error("failed to render template '{template}': {source}")]
    RenderFailed {
        template: String,
        #[source]
        source: askama::Error,
    },
    #[error("failed to write config file at {path}: {source}")]
    WriteFailed {
        path: String,
        #[source]
        source: std::io::Error,
    },
}

pub struct ConfigGenerator {
    deployment_dir: PathBuf,
}

impl ConfigGenerator {
    pub fn new(deployment_dir: impl Into<PathBuf>) -> Self {
        Self {
            deployment_dir: deployment_dir.into(),
        }
    }

    /// Validate all template parameters and render all desired config files.
    /// The returned paths are relative to the deployment directory.
    pub fn render_all(&self, params: &ObservabilityParams) -> Result<Vec<RenderedConfigFile>, ConfigGenError> {
        self.validate(params)?;
        Ok(vec![
            self.render_alloy(params)?,
            self.render_mimir(params)?,
            self.render_loki(params)?,
            self.render_grafana_datasources(params)?,
            self.render_grafana_dashboard_provider()?,
            self.grpc_edge_dashboard(),
            self.broker_runtime_dashboard(),
            self.storage_projection_dashboard(),
        ])
    }

    fn validate(&self, params: &ObservabilityParams) -> Result<(), ConfigGenError> {
        if params.metrics_target_host.is_empty() {
            return Err(ConfigGenError::InvalidParameter {
                field: "metrics_target_host".into(),
                reason: "empty".into(),
            });
        }
        if params.metrics_target_port == 0 {
            return Err(ConfigGenError::InvalidParameter {
                field: "metrics_target_port".into(),
                reason: "zero".into(),
            });
        }
        if params.mimir_remote_write_url.is_empty() {
            return Err(ConfigGenError::InvalidParameter {
                field: "mimir_remote_write_url".into(),
                reason: "empty".into(),
            });
        }
        if params.loki_push_url.is_empty() {
            return Err(ConfigGenError::InvalidParameter {
                field: "loki_push_url".into(),
                reason: "empty".into(),
            });
        }
        if params.mimir_http_port == 0 {
            return Err(ConfigGenError::InvalidParameter {
                field: "mimir_http_port".into(),
                reason: "zero".into(),
            });
        }
        if params.loki_http_port == 0 {
            return Err(ConfigGenError::InvalidParameter {
                field: "loki_http_port".into(),
                reason: "zero".into(),
            });
        }
        Ok(())
    }

    fn config_dir(&self) -> PathBuf {
        self.deployment_dir.join("config")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderedConfigFile {
    pub relative_path: PathBuf,
    pub contents: String,
}
```

### 3. Observability Config Files Resource (`platforms/compose/src/observability_config.rs`)

The config files are managed by a resource so plan is read-only and apply/destroy are stateful IaC operations. The resource ID is stable: `compose/observability-config-files`.

```rust
#[derive(Debug, Clone)]
pub struct ObservabilityConfigFilesResource {
    deployment_dir: PathBuf,
    params: ObservabilityParams,
}

impl ObservabilityConfigFilesResource {
    pub fn desired_files(&self) -> Result<Vec<RenderedConfigFile>, ConfigGenError> {
        ConfigGenerator::new(&self.deployment_dir).render_all(&self.params)
    }

    fn ensure_directories(&self) -> Result<(), ConfigGenError> {
        for dir in [
            self.deployment_dir.join("config"),
            self.deployment_dir.join("config/grafana/provisioning/datasources"),
            self.deployment_dir.join("config/grafana/provisioning/dashboards"),
            self.deployment_dir.join("config/grafana/dashboards"),
        ] {
            std::fs::create_dir_all(&dir).map_err(|source| ConfigGenError::WriteFailed {
                path: dir.display().to_string(),
                source,
            })?;
        }
        Ok(())
    }
}
```

`describe()` reads the managed file paths and stores per-file checksums in `ResourceState`. `diff()` compares those checksums with `desired_files()`. `create()` and `update()` call `ensure_directories()` and then write each desired file. `delete()` removes only the managed files listed by `desired_files()` and then prunes generated directories if they are empty. The IaC resources for `mimir`, `loki`, `grafana`, and `alloy` depend on `compose/observability-config-files`, so apply writes config before containers are reconciled.

### 4. ObservabilityParams

All dynamic parameters needed to render the observability config set, derived from `ComposeConfig`.

```rust
/// All dynamic parameters needed to render the observability config set.
#[derive(Debug, Clone)]
pub struct ObservabilityParams {
    pub metrics_target_host: String,
    pub metrics_target_port: u16,
    pub mimir_remote_write_url: String,
    pub loki_push_url: String,
    pub mimir_http_port: u16,
    pub loki_http_port: u16,
    pub loki_retention_hours: u32,
}

impl ObservabilityParams {
    /// Derive parameters from the compose platform config.
    pub fn from_config(config: &ComposeConfig) -> Self {
        Self {
            metrics_target_host: "tokeirad".into(),
            metrics_target_port: config.tokeirad.metrics_port,
            mimir_remote_write_url: "http://mimir:9009/api/v1/push".into(),
            loki_push_url: "http://loki:3100/loki/api/v1/push".into(),
            mimir_http_port: 9009,
            loki_http_port: 3100,
            loki_retention_hours: 168,
        }
    }
}
```

### 5. ComposeService Integration

The existing `compose_services()` function in `platforms/compose/src/compose.rs` is extended to include config volume mounts and command overrides for observability services.

```rust
ComposeService {
    name: "mimir".into(),
    image: config.observability.mimir_image.clone(),
    ports: vec!["9009:9009".into()],
    volumes: vec![
        mimir_data_vol,
        format!("{}/config/mimir.yaml:/etc/mimir/mimir.yaml", deploy_dir),
    ],
    command: vec!["--config.file=/etc/mimir/mimir.yaml".into()],
    ..
}

ComposeService {
    name: "loki".into(),
    image: config.observability.loki_image.clone(),
    ports: vec!["3100:3100".into()],
    volumes: vec![
        loki_data_vol,
        format!("{}/config/loki.yaml:/etc/loki/loki.yaml", deploy_dir),
    ],
    command: vec!["--config.file=/etc/loki/loki.yaml".into()],
    ..
}

ComposeService {
    name: "alloy".into(),
    image: config.observability.alloy_image.clone(),
    ports: vec!["4317:4317".into(), "4318:4318".into()],
    volumes: vec![
        "/var/run/docker.sock:/var/run/docker.sock".into(),
        format!("{}/config/alloy.alloy:/etc/alloy/config.alloy", deploy_dir),
    ],
    command: vec!["run".into(), "/etc/alloy/config.alloy".into()],
    ..
}

ComposeService {
    name: "grafana".into(),
    image: config.observability.grafana_image.clone(),
    ports: vec![format!("{}:{}", grafana_port, grafana_port)],
    volumes: vec![
        grafana_data_vol,
        format!("{}/config/grafana/provisioning/:/etc/grafana/provisioning/", deploy_dir),
        format!("{}/config/grafana/dashboards/:/var/lib/grafana/dashboards/", deploy_dir),
    ],
    ..
}
```

### 6. Module Integration

The observability module constructor remains infallible because `Deployment::infra_modules()` returns `Vec<Box<dyn Module>>`. It creates the config-file resource and compose service resources; rendering and file I/O happen inside the config-file resource lifecycle methods.

```rust
impl ComposeModule {
    pub fn observability(config: &ComposeConfig) -> Self {
        let params = ObservabilityParams::from_config(config);
        let services: Vec<ComposeService> = compose_services(config)
            .into_iter()
            .filter(|s| module_for_service(&s.name) == MODULE_OBSERVABILITY)
            .collect();

        Self {
            module_name: MODULE_OBSERVABILITY.into(),
            config_files: ObservabilityConfigFilesResource::new(
                config.deployment_dir.clone(),
                params,
            ),
            services,
        }
    }
}
```

`Module::resources()` returns the config-file resource first, then the compose service resources. The `OwnedComposeResource` wrapper appends `compose/observability-config-files` to the IaC resource dependencies for `mimir`, `loki`, `grafana`, and `alloy` without adding that ID to Docker Compose `depends_on`; the file resource is not a compose service.

## Data Models

### ObservabilityParams

| Field | Type | Source | Validation |
|-------|------|--------|------------|
| `metrics_target_host` | `String` | `"tokeirad"` (compose service DNS name) | Non-empty |
| `metrics_target_port` | `u16` | `config.tokeirad.metrics_port` | Non-zero |
| `mimir_remote_write_url` | `String` | `"http://mimir:9009/api/v1/push"` | Non-empty |
| `loki_push_url` | `String` | `"http://loki:3100/loki/api/v1/push"` | Non-empty |
| `mimir_http_port` | `u16` | `9009` | Non-zero |
| `loki_http_port` | `u16` | `3100` | Non-zero |
| `loki_retention_hours` | `u32` | `168` (7 days) | — |

### ConfigGenError

| Variant | Fields | When |
|---------|--------|------|
| `InvalidParameter` | `field`, `reason` | Validation fails before any I/O |
| `RenderFailed` | `template`, `source` | Askama template rendering fails |
| `WriteFailed` | `path`, `source` | Directory creation or file write fails |

### Generated File Layout

```
<deployment_dir>/
└── config/
    ├── alloy.alloy
    ├── mimir.yaml
    ├── loki.yaml
    └── grafana/
        ├── provisioning/
        │   ├── datasources/
        │   │   └── datasources.yaml
        │   └── dashboards/
        │       └── dashboards.yaml
        └── dashboards/
            ├── grpc-edge-health.json
            ├── broker-runtime-health.json
            └── storage-projection-health.json
```

## Template Files (`platforms/compose/templates/`)

### `alloy.alloy`

```
prometheus.scrape "tokeirad" {
  targets         = [{ __address__ = "{{ metrics_target_host }}:{{ metrics_target_port }}" }]
  forward_to      = [prometheus.remote_write.mimir.receiver]
  scrape_interval = "15s"
  job_name        = "tokeirad"
  metrics_path    = "/metrics"
}

prometheus.remote_write "mimir" {
  endpoint {
    url = "{{ mimir_remote_write_url }}"
  }
}

discovery.docker "containers" {
  host = "unix:///var/run/docker.sock"
}

discovery.relabel "docker_logs" {
  targets = discovery.docker.containers.targets

  rule {
    source_labels = ["__meta_docker_container_name"]
    regex         = "/(.*)"
    target_label  = "container"
  }

  rule {
    source_labels = ["__meta_docker_container_label_com_docker_compose_service"]
    target_label  = "service"
  }
}

loki.source.docker "containers" {
  host       = "unix:///var/run/docker.sock"
  targets    = discovery.relabel.docker_logs.output
  forward_to = [loki.write.local.receiver]
}

loki.write "local" {
  endpoint {
    url = "{{ loki_push_url }}"
  }
}
```

### `mimir.yaml`

```yaml
target: all

multitenancy_enabled: false

server:
  http_listen_port: {{ http_port }}
  grpc_listen_port: 9095

common:
  storage:
    backend: filesystem
    filesystem:
      dir: /data/mimir

blocks_storage:
  backend: filesystem
  filesystem:
    dir: /data/mimir/blocks
  tsdb:
    dir: /data/mimir/tsdb

compactor:
  data_dir: /data/mimir/compactor
  sharding_ring:
    kvstore:
      store: memberlist

distributor:
  ring:
    kvstore:
      store: memberlist

ingester:
  ring:
    kvstore:
      store: memberlist
    replication_factor: 1

ruler_storage:
  backend: filesystem
  filesystem:
    dir: /data/mimir/rules

store_gateway:
  sharding_ring:
    replication_factor: 1

limits:
  max_global_series_per_user: 500000
  max_global_series_per_metric: 50000
  ingestion_rate: 100000
  ingestion_burst_size: 200000
```

### `loki.yaml`

```yaml
target: all

auth_enabled: false

server:
  http_listen_port: {{ http_port }}

common:
  instance_addr: 127.0.0.1
  path_prefix: /loki
  storage:
    filesystem:
      chunks_directory: /loki/chunks
      rules_directory: /loki/rules
  replication_factor: 1
  ring:
    kvstore:
      store: inmemory

schema_config:
  configs:
    - from: "2024-01-01"
      store: tsdb
      object_store: filesystem
      schema: v13
      index:
        prefix: index_
        period: 24h

limits_config:
  retention_period: {{ retention_hours }}h
  max_query_length: 721h
  max_query_series: 100000

compactor:
  working_directory: /loki/compactor
  retention_enabled: true
  delete_request_store: filesystem
```

### `grafana-datasources.yaml`

```yaml
apiVersion: 1

datasources:
  - name: Mimir
    type: prometheus
    uid: mimir
    access: proxy
    url: {{ mimir_url }}
    isDefault: true
    editable: true
    jsonData:
      httpMethod: POST
      prometheusType: Mimir

  - name: Loki
    type: loki
    uid: loki
    access: proxy
    url: {{ loki_url }}
    editable: true
```

### `grafana-dashboards.yaml`

```yaml
apiVersion: 1

providers:
  - name: 'Tokeira'
    orgId: 1
    folder: 'Tokeira'
    folderUid: 'tokeira'
    type: file
    disableDeletion: true
    updateIntervalSeconds: 30
    allowUiUpdates: false
    options:
      path: {{ dashboards_path }}
```

## Dashboard Specifications

Dashboard JSON files are static assets (not templated) because their content is fixed metric queries. They are written as embedded `include_str!` constants or generated programmatically from a `DashboardBuilder` helper.

Each dashboard includes a `tokeira_build_info` stat panel (version + commit labels) plus domain-specific panels.

### gRPC/Edge Health Dashboard (`grpc-edge-health.json`)

| Panel | Query |
|-------|-------|
| Build Info | `tokeira_build_info` (stat, version+commit labels) |
| Request Rate | `rate(tokeira_edge_grpc_request_total[5m])` |
| Error Rate | `rate(tokeira_edge_grpc_error_total[5m])` |
| Error Ratio | `rate(tokeira_edge_grpc_error_total[5m]) / rate(tokeira_edge_grpc_request_total[5m])` |
| Latency p50/p95/p99 | `histogram_quantile(0.5\|0.95\|0.99, rate(tokeira_edge_grpc_request_duration_seconds_bucket[5m]))` |
| Active Requests | `tokeira_edge_grpc_active_requests` |

### Broker/Runtime Health Dashboard (`broker-runtime-health.json`)

| Panel | Query |
|-------|-------|
| Build Info | `tokeira_build_info` |
| Publish Rate | `rate(tokeira_runtime_broker_publish_total[5m])` |
| Sync Match Rate | `rate(tokeira_runtime_broker_sync_match_total[5m])` |
| Non-Sync Match Rate | `rate(tokeira_runtime_broker_non_sync_match_total[5m])` |
| Poll Timeout Rate | `rate(tokeira_runtime_broker_poll_timeout_total[5m])` |
| Queue Depth | `tokeira_runtime_broker_queue_depth` |
| Lane Submit Latency p50/p95/p99 | `histogram_quantile(0.5\|0.95\|0.99, rate(tokeira_runtime_lane_submit_duration_seconds_bucket[5m]))` |
| Scanner Tick Rate | `rate(tokeira_runtime_scanner_tick_total[5m])` |
| Scanner Dispatched Rate | `rate(tokeira_runtime_scanner_dispatched_total[5m])` |
| OCC Retry Rate | `rate(tokeira_runtime_occ_retry_total[5m])` |

### Storage/Projection Health Dashboard (`storage-projection-health.json`)

| Panel | Query |
|-------|-------|
| Build Info | `tokeira_build_info` |
| Commit Transition Latency p50/p95/p99 | `histogram_quantile(0.5\|0.95\|0.99, rate(tokeira_storage_commit_transition_duration_seconds_bucket[5m]))` |
| Load Run Latency p50/p95/p99 | `histogram_quantile(0.5\|0.95\|0.99, rate(tokeira_storage_load_run_duration_seconds_bucket[5m]))` |
| Read History Latency p50/p95/p99 | `histogram_quantile(0.5\|0.95\|0.99, rate(tokeira_storage_read_history_duration_seconds_bucket[5m]))` |
| Storage Operations by Type | `rate(tokeira_storage_repository_operation_total[5m])` (by `operation` label) |
| Projection Records Processed | `rate(tokeira_projection_records_processed_total[5m])` |
| Projection Lag | `tokeira_projection_worker_lag_records` |
| Sink Write Latency p50/p95/p99 | `histogram_quantile(0.5\|0.95\|0.99, rate(tokeira_projection_sink_write_duration_seconds_bucket[5m]))` |
| Sink Error Rate | `rate(tokeira_projection_sink_error_total[5m])` |

## Error Handling

| Failure | Behavior |
|---------|----------|
| Invalid parameter (empty URL, zero port) | `ConfigGenError::InvalidParameter` returned before any file I/O |
| Template render failure | `ConfigGenError::RenderFailed` from the config resource; dependent service apply halts |
| Directory creation failure | `ConfigGenError::WriteFailed` from the config resource; dependent service apply halts |
| File write failure | `ConfigGenError::WriteFailed` from the config resource; dependent service apply halts |
| Any `ConfigGenError` during config resource create/update | Propagated as `IacError::Other`; dependent compose services are not created or updated |

## Testing Strategy

### Property-Based Tests (proptest)

- Generate random valid `ObservabilityParams` and verify rendered output contains all parameter values
- Generate random deployment directory paths and verify volume mount consistency
- Generate invalid params (empty strings, zero ports) and verify rejection
- Generate valid params with a read-only temp directory and verify config resource error propagation

### Unit Tests

- Each template renders without error with default params
- Rendered Alloy config contains `prometheus.scrape`, `prometheus.remote_write`, `loki.source.docker`, `loki.write` components
- Rendered Mimir config is valid YAML with `target: all` equivalent (single-binary mode)
- Rendered Loki config is valid YAML with correct retention
- Dashboard JSON files are valid JSON with expected panel structure
- `compose_services()` produces correct volume mounts and commands for each service
- `ObservabilityConfigFilesResource::diff()` reports missing or changed managed files
- `ObservabilityConfigFilesResource::create()` writes files and dependent compose services depend on it

### Integration Tests

- Full config resource apply against a temp directory produces all 8 expected files
- Module apply with valid config succeeds end-to-end
- Module apply with invalid config returns an error from the config resource before creating containers

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system — essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: Template parameter injection

*For any* valid `ObservabilityParams` (non-empty URLs, non-zero ports, non-empty hostname), rendering each template SHALL produce output that contains the exact parameter values: the metrics target host and port appear in the Alloy config, the Mimir remote-write URL appears in the Alloy config, the Loki push URL appears in the Alloy config, the Mimir HTTP port appears in the Mimir config, the Loki HTTP port and retention hours appear in the Loki config, and the Mimir/Loki query URLs appear in the datasources config.

**Validates: Requirements 1.6, 2.4, 3.4, 3.5, 4.2, 4.3**

### Property 2: Volume mount path consistency

*For any* valid deployment directory path, the volume mounts on each observability ComposeService SHALL reference paths under `<deployment_dir>/config/` that correspond exactly to the file paths where `ObservabilityConfigFilesResource` writes its output — ensuring that the container mount source matches the generated file location.

**Validates: Requirements 9.1, 9.2, 9.3, 9.4**

### Property 3: Invalid parameters are rejected

*For any* `ObservabilityParams` where at least one URL field is empty or at least one port field is zero, `ConfigGenerator::render_all()` SHALL return a `ConfigGenError::InvalidParameter` error before any file I/O occurs.

**Validates: Requirements 11.3**

### Property 4: Rendering failure halts execution

*For any* configuration resource create/update that produces an error (simulated via an invalid template path or I/O failure), the observability module apply SHALL return an error from `ObservabilityConfigFilesResource` and SHALL NOT create or update dependent ComposeService resources.

**Validates: Requirements 12.6**

### Property 5: Config file output completeness

*For any* valid `ObservabilityParams`, `ConfigGenerator::render_all()` SHALL produce exactly the expected set of rendered file entries: `config/alloy.alloy`, `config/mimir.yaml`, `config/loki.yaml`, `config/grafana/provisioning/datasources/datasources.yaml`, `config/grafana/provisioning/dashboards/dashboards.yaml`, `config/grafana/dashboards/grpc-edge-health.json`, `config/grafana/dashboards/broker-runtime-health.json`, `config/grafana/dashboards/storage-projection-health.json`.

**Validates: Requirements 1.1, 2.1, 3.1, 4.1, 5.1, 6.1, 7.1, 8.1**
