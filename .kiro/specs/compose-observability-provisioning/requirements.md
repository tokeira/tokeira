# Requirements Document

## Introduction

Provision configuration files for the compose platform's observability stack (Alloy, Mimir, Loki, Grafana) during the IaC observability module apply step. Configuration files are generated from Askama templates with typed template structs, written to the deployment directory's `config/` subdirectory, and bind-mounted into the corresponding Docker Compose services. Three pre-built Grafana dashboards provide focused views of gRPC/edge health, broker/runtime health, and storage/projection health using tokeirad's Prometheus metrics.

## Glossary

- **Config_Generator**: The component responsible for validating parameters and rendering Askama templates into deterministic file contents.
- **Observability_Config_Files_Resource**: The IaC resource owned by the Observability_Module that diffs, writes, refreshes, and deletes the generated observability config files.
- **Alloy_Config**: The Grafana Alloy River configuration file that defines metrics scraping and log forwarding pipelines.
- **Mimir_Config**: The Grafana Mimir YAML configuration file for single-binary mode with filesystem storage.
- **Loki_Config**: The Grafana Loki YAML configuration file for single-binary mode with filesystem storage and retention policy.
- **Grafana_Provisioning**: The set of YAML files that configure Grafana datasources and dashboard providers on startup.
- **Dashboard_JSON**: A Grafana dashboard definition file in JSON format containing panel layouts, queries, and thresholds.
- **Deployment_Directory**: The filesystem path where `docker-compose.yml` and associated configuration reside.
- **Observability_Module**: The IaC module that owns the mimir, loki, grafana, and alloy compose services.
- **ComposeService**: The Rust struct representing a Docker Compose service descriptor with name, image, ports, volumes, environment, depends_on, and healthcheck fields.

## Requirements

### Requirement 1: Alloy Configuration Generation

**User Story:** As an operator, I want Alloy to be configured to scrape tokeirad metrics and forward container logs, so that metrics reach Mimir and logs reach Loki without manual configuration.

#### Acceptance Criteria

1. WHEN the Observability_Config_Files_Resource is created or updated during apply, THE Observability_Config_Files_Resource SHALL write the Alloy_Config rendered by the Config_Generator to `<Deployment_Directory>/config/alloy.alloy`.
2. THE Alloy_Config SHALL define a `prometheus.scrape` component targeting the compose `tokeirad` service metrics endpoint on the configured metrics port at the `/metrics` path.
3. THE Alloy_Config SHALL define a `prometheus.remote_write` component sending metrics to Mimir at `http://mimir:9009/api/v1/push`.
4. THE Alloy_Config SHALL define a `loki.source.docker` component reading container logs from the Docker socket.
5. THE Alloy_Config SHALL define a `loki.write` component forwarding logs to Loki at `http://loki:3100/loki/api/v1/push`.
6. THE Alloy_Config SHALL use template parameters for the tokeirad metrics target hostname and port, the Mimir remote-write URL, and the Loki push URL.

### Requirement 2: Mimir Configuration Generation

**User Story:** As an operator, I want Mimir configured in single-binary mode with filesystem storage, so that metrics are persisted locally without external dependencies.

#### Acceptance Criteria

1. WHEN the Observability_Config_Files_Resource is created or updated during apply, THE Observability_Config_Files_Resource SHALL write the Mimir_Config rendered by the Config_Generator to `<Deployment_Directory>/config/mimir.yaml`.
2. THE Mimir_Config SHALL configure Mimir to run in single-binary mode (`target: all`).
3. THE Mimir_Config SHALL configure filesystem-based block storage at the `/data` path.
4. THE Mimir_Config SHALL configure the HTTP listen port as 9009.

### Requirement 3: Loki Configuration Generation

**User Story:** As an operator, I want Loki configured in single-binary mode with filesystem storage and 7-day retention, so that logs are persisted locally with bounded disk usage.

#### Acceptance Criteria

1. WHEN the Observability_Config_Files_Resource is created or updated during apply, THE Observability_Config_Files_Resource SHALL write the Loki_Config rendered by the Config_Generator to `<Deployment_Directory>/config/loki.yaml`.
2. THE Loki_Config SHALL configure Loki to run in single-binary mode (`target: all`).
3. THE Loki_Config SHALL configure filesystem-based storage at the `/loki` path.
4. THE Loki_Config SHALL configure a retention period of 7 days (168 hours).
5. THE Loki_Config SHALL configure the HTTP listen port as 3100.

### Requirement 4: Grafana Datasource Provisioning

**User Story:** As an operator, I want Grafana to auto-discover Mimir and Loki as datasources on startup, so that dashboards work immediately without manual datasource configuration.

#### Acceptance Criteria

1. WHEN the Observability_Config_Files_Resource is created or updated during apply, THE Observability_Config_Files_Resource SHALL write the datasource provisioning YAML rendered by the Config_Generator to `<Deployment_Directory>/config/grafana/provisioning/datasources/datasources.yaml`.
2. THE datasource provisioning file SHALL define a Prometheus-type datasource named "Mimir" with URL `http://mimir:9009/prometheus` set as the default datasource.
3. THE datasource provisioning file SHALL define a Loki-type datasource named "Loki" with URL `http://loki:3100`.

### Requirement 5: Grafana Dashboard Provider Provisioning

**User Story:** As an operator, I want Grafana to load dashboards from a provisioned directory on startup, so that pre-built dashboards are available without manual import.

#### Acceptance Criteria

1. WHEN the Observability_Config_Files_Resource is created or updated during apply, THE Observability_Config_Files_Resource SHALL write the dashboard provider YAML rendered by the Config_Generator to `<Deployment_Directory>/config/grafana/provisioning/dashboards/dashboards.yaml`.
2. THE dashboard provider file SHALL configure a file-based provider pointing to `/var/lib/grafana/dashboards` inside the Grafana container.
3. THE dashboard provider file SHALL set `disableDeletion` to true and `updateIntervalSeconds` to 30.

### Requirement 6: gRPC/Edge Health Dashboard

**User Story:** As an operator, I want a pre-built dashboard showing gRPC request rates, error rates, latency percentiles, and active request counts, so that I can monitor edge layer health at a glance.

#### Acceptance Criteria

1. WHEN the Observability_Config_Files_Resource is created or updated during apply, THE Observability_Config_Files_Resource SHALL write a Dashboard_JSON file for gRPC/edge health to `<Deployment_Directory>/config/grafana/dashboards/grpc-edge-health.json`.
2. THE gRPC/edge health dashboard SHALL include panels for `tokeira_edge_grpc_request_total` rate, `tokeira_edge_grpc_error_total` rate, `tokeira_edge_grpc_request_duration_seconds` histogram percentiles (p50, p95, p99), and `tokeira_edge_grpc_active_requests` gauge.
3. THE gRPC/edge health dashboard SHALL include a panel for error ratio computed as `tokeira_edge_grpc_error_total` rate divided by `tokeira_edge_grpc_request_total` rate.

### Requirement 7: Broker/Runtime Health Dashboard

**User Story:** As an operator, I want a pre-built dashboard showing broker throughput, queue depth, lane submit latency, scanner activity, and OCC retries, so that I can monitor runtime health at a glance.

#### Acceptance Criteria

1. WHEN the Observability_Config_Files_Resource is created or updated during apply, THE Observability_Config_Files_Resource SHALL write a Dashboard_JSON file for broker/runtime health to `<Deployment_Directory>/config/grafana/dashboards/broker-runtime-health.json`.
2. THE broker/runtime health dashboard SHALL include panels for `tokeira_runtime_broker_publish_total` rate, `tokeira_runtime_broker_sync_match_total` rate, `tokeira_runtime_broker_non_sync_match_total` rate, and `tokeira_runtime_broker_poll_timeout_total` rate.
3. THE broker/runtime health dashboard SHALL include a panel for `tokeira_runtime_broker_queue_depth` gauge.
4. THE broker/runtime health dashboard SHALL include a panel for `tokeira_runtime_lane_submit_duration_seconds` histogram percentiles (p50, p95, p99).
5. THE broker/runtime health dashboard SHALL include panels for `tokeira_runtime_scanner_tick_total` rate, `tokeira_runtime_scanner_dispatched_total` rate, and `tokeira_runtime_occ_retry_total` rate.

### Requirement 8: Storage/Projection Health Dashboard

**User Story:** As an operator, I want a pre-built dashboard showing storage operation latencies, operation counts, projection lag, and sink health, so that I can monitor persistence and projection health at a glance.

#### Acceptance Criteria

1. WHEN the Observability_Config_Files_Resource is created or updated during apply, THE Observability_Config_Files_Resource SHALL write a Dashboard_JSON file for storage/projection health to `<Deployment_Directory>/config/grafana/dashboards/storage-projection-health.json`.
2. THE storage/projection health dashboard SHALL include panels for `tokeira_storage_commit_transition_duration_seconds`, `tokeira_storage_load_run_duration_seconds`, and `tokeira_storage_read_history_duration_seconds` histogram percentiles (p50, p95, p99).
3. THE storage/projection health dashboard SHALL include a panel for `tokeira_storage_repository_operation_total` rate broken down by operation label.
4. THE storage/projection health dashboard SHALL include panels for `tokeira_projection_records_processed_total` rate, `tokeira_projection_worker_lag_records` gauge, `tokeira_projection_sink_write_duration_seconds` histogram percentiles, and `tokeira_projection_sink_error_total` rate.

### Requirement 9: Compose Service Volume Mounts

**User Story:** As an operator, I want observability services to have their configuration files bind-mounted from the deployment directory, so that generated configs are used at container startup.

#### Acceptance Criteria

1. THE ComposeService for "mimir" SHALL include a volume mount mapping `<Deployment_Directory>/config/mimir.yaml` to `/etc/mimir/mimir.yaml` inside the container.
2. THE ComposeService for "loki" SHALL include a volume mount mapping `<Deployment_Directory>/config/loki.yaml` to `/etc/loki/loki.yaml` inside the container.
3. THE ComposeService for "alloy" SHALL include a volume mount mapping `<Deployment_Directory>/config/alloy.alloy` to `/etc/alloy/config.alloy` inside the container.
4. THE ComposeService for "grafana" SHALL include volume mounts mapping the `<Deployment_Directory>/config/grafana/provisioning/` directory to `/etc/grafana/provisioning/` and the `<Deployment_Directory>/config/grafana/dashboards/` directory to `/var/lib/grafana/dashboards/` inside the container.

### Requirement 10: Compose Service Command Overrides

**User Story:** As an operator, I want observability services to start with command arguments pointing to their configuration files, so that each service loads the correct generated configuration.

#### Acceptance Criteria

1. THE ComposeService for "mimir" SHALL include a command override specifying `--config.file=/etc/mimir/mimir.yaml`.
2. THE ComposeService for "loki" SHALL include a command override specifying `--config.file=/etc/loki/loki.yaml`.
3. THE ComposeService for "alloy" SHALL include a command override specifying `run /etc/alloy/config.alloy`.

### Requirement 11: Askama Template Implementation

**User Story:** As a developer, I want configuration templates implemented as Askama typed template structs, so that template rendering is type-safe and compile-time verified.

#### Acceptance Criteria

1. THE Config_Generator SHALL implement each configuration template as a Rust struct deriving `askama::Template` with a `#[template]` attribute pointing to the template source file.
2. THE Config_Generator SHALL define typed fields on each template struct for all dynamic values (metrics target host, metrics target port, Mimir remote-write URL, Loki push URL, Mimir HTTP port, Loki HTTP port, Loki retention period).
3. IF a template struct field value is invalid (empty string for a URL field, zero for a port field), THEN THE Config_Generator SHALL return an error before attempting to write the rendered output.

### Requirement 12: Config File Resource Lifecycle

**User Story:** As an operator, I want configuration files managed by IaC state, so that plans can show drift and apply writes configs before services start.

#### Acceptance Criteria

1. THE Observability_Module SHALL include an Observability_Config_Files_Resource in its desired resources.
2. THE mimir, loki, grafana, and alloy IaC resources SHALL depend on the Observability_Config_Files_Resource so apply writes config files before creating or updating the containers.
3. WHEN planning, THE Observability_Config_Files_Resource SHALL render desired file contents and compare them with files on disk without writing any files.
4. WHEN applying create or update, THE Observability_Config_Files_Resource SHALL create the `<Deployment_Directory>/config/` directory and all required subdirectories before writing any configuration files.
5. WHEN applying create or update, THE Observability_Config_Files_Resource SHALL render and write all configuration files (Alloy, Mimir, Loki, Grafana provisioning, dashboards) before the compose services are created or updated.
6. IF any configuration file fails to render or write, THEN the Observability_Config_Files_Resource SHALL return an error and the Observability_Module apply step SHALL halt before creating or updating compose services.
7. WHEN refreshing state, THE Observability_Config_Files_Resource SHALL report drift if any managed file is missing or its checksum differs from the rendered desired content.
8. WHEN deleting the observability module, THE Observability_Config_Files_Resource SHALL remove the managed files and empty generated subdirectories it owns without deleting unrelated files under `<Deployment_Directory>/config/`.

### Requirement 13: Build Info Dashboard Panel

**User Story:** As an operator, I want each dashboard to display the tokeirad build version, so that I can correlate metrics with the deployed version.

#### Acceptance Criteria

1. THE gRPC/edge health dashboard, broker/runtime health dashboard, and storage/projection health dashboard SHALL each include a stat panel displaying the `tokeira_build_info` gauge with version and commit labels.
