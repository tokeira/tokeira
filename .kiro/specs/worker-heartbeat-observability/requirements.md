# Requirements Document: Worker Heartbeat Observability

## Introduction

`temporal-api-v1.62-sync` lands `RecordWorkerHeartbeat` as an accept-and-discard handler: it accepts the upstream `Vec<temporal::api::worker::v1::WorkerHeartbeat>`, validates that the request namespace is non-empty, emits one `debug!` line per call, and returns `Ok(RecordWorkerHeartbeatResponse {})`.

This spec promotes that no-op into real observability without changing SDK-visible RPC behaviour. The edge decodes each upstream heartbeat into a compact shared model in `tokeira-types`, the runtime stores the latest heartbeat per worker in an in-memory registry, and operator metrics expose heartbeat acceptance, active worker counts, and heartbeat staleness. Heartbeats remain observations, not correctness state: no kernel transition and no DSQL persistence are introduced.

The design deliberately avoids crate cycles. `tokeira-edge`, `tokeira-runtime`, and future worker-query code all depend on `tokeira-types`; therefore the shared `WorkerHeartbeat`, `WorkerInstanceKey`, `HeartbeatStore`, `HeartbeatStoreError`, and `EvictionReport` definitions live there. The runtime owns the default in-memory implementation. The edge owns proto decoding and handler orchestration.

## Scope

### Delivered

- A compact `tokeira-types::WorkerHeartbeat` model carrying worker liveness/query fields.
- A `tokeira-types::WorkerInstanceKey` newtype.
- A neutral `tokeira-types::HeartbeatStore` trait.
- A runtime `InMemoryHeartbeatStore` implementation.
- A decode-only edge translator from upstream `temporal.api.worker.v1.WorkerHeartbeat` to the shared model.
- `record_worker_heartbeat` and `shutdown_worker` handler migration to insert decoded heartbeats into the store.
- Runtime metrics for heartbeat acceptance, active workers, total workers, active-state, and staleness.
- A stable data-source contract for the future `worker-deployments` `ListWorkers` / `DescribeWorker` implementation.
- A Surface_Audit amendment for `WorkflowService.RecordWorkerHeartbeat`.

### Deferred

- `ListWorkers` and `DescribeWorker` RPC handlers.
- Encode translator from the shared model back to upstream proto.
- Full-fidelity worker sub-message preservation (`WorkerHostInfo`, `WorkerSlotsInfo`, `WorkerPollerInfo`, `PluginInfo`, `StorageDriverInfo`) unless future SDK-visible response work requires it.
- Worker deployment/version DTOs beyond compact `build_id` and `deployment_name` hints.
- Query filtering, worker identity normalization, heartbeat-driven admission control, and historical heartbeat archival.
- DSQL-backed heartbeat persistence. Loss on process restart is acceptable and matches upstream Temporal's in-memory worker registry.

## Glossary

- **WorkerHeartbeat_Upstream**: The upstream protobuf message `temporal.api.worker.v1.WorkerHeartbeat`.
- **WorkerHeartbeat_Model**: The compact `tokeira-types::WorkerHeartbeat` model introduced by this spec.
- **WorkerInstanceKey**: Opaque worker-process identifier sourced from `WorkerHeartbeat.worker_instance_key`.
- **HeartbeatStore**: Neutral trait in `tokeira-types` used by edge/runtime/query code without creating crate cycles.
- **InMemoryHeartbeatStore**: Runtime-owned default implementation of `HeartbeatStore`.
- **Heartbeat_Metrics**: Operator metrics emitted by the runtime heartbeat store and maintenance loop.
- **Handler_Behaviour_Parity**: `RecordWorkerHeartbeat`, `ShutdownWorker`, `DescribeNamespace`, and `GetSystemInfo` remain byte/status-compatible with v1.62-sync from the SDK's perspective.

## Requirements

---

## Feature 1: Shared Heartbeat Model

### Requirement 1.1: Define shared heartbeat types in `tokeira-types`

**User Story:** As a Tokeira developer, I want heartbeat records represented in a neutral shared crate, so that edge, runtime, and future query code can share the data without dependency cycles.

#### Acceptance Criteria

1. THE `WorkerHeartbeat`, `WorkerInstanceKey`, `HeartbeatStore`, `HeartbeatStoreError`, and `EvictionReport` definitions SHALL live in `crates/tokeira-types`.
2. `WorkerInstanceKey` SHALL be a newtype over `String` and derive `Debug`, `Clone`, `PartialEq`, `Eq`, `Hash`, `Serialize`, and `Deserialize`.
3. `WorkerHeartbeat` SHALL derive `Debug`, `Clone`, `PartialEq`, `Eq`, `Serialize`, and `Deserialize`.
4. `WorkerHeartbeat` SHALL contain:
   - `namespace_id: NamespaceId`
   - `worker_instance_key: WorkerInstanceKey`
   - `task_queue: TaskQueueName`
   - `worker_identity: WorkerIdentity`
   - `last_seen: time::OffsetDateTime`
   - `status: WorkerHeartbeatStatus`
   - `build_id: Option<String>`
   - `deployment_name: Option<String>`
   - `sdk_name: Option<String>`
   - `sdk_version: Option<String>`
5. `WorkerHeartbeatStatus` SHALL be a compact shared enum or newtype in `tokeira-types` that preserves the upstream worker status value, including shutdown/final heartbeat status.
6. `deployment_name` SHALL map directly from upstream `WorkerDeploymentVersion.deployment_name`; the spec SHALL NOT introduce a `deployment_series_name` alias.
7. The model SHALL use existing codebase conventions: `time::OffsetDateTime` for timestamps and existing `tokeira-types` domain newtypes for identifiers.
8. The model SHALL NOT use proto-layer types, nonexistent `tokeira_types::Timestamp` / `tokeira_types::Duration` aliases, or a nonexistent `WorkerDeploymentVersion` DTO.

### Requirement 1.2: Define the neutral `HeartbeatStore` trait

**User Story:** As a Tokeira developer, I want a minimal store trait in `tokeira-types`, so that the runtime can provide the implementation while edge and future query code depend only on the contract.

#### Acceptance Criteria

1. `HeartbeatStore` SHALL be `Send + Sync + 'static`.
2. `HeartbeatStore` SHALL expose:
   - `insert(&self, heartbeat: WorkerHeartbeat) -> Result<(), HeartbeatStoreError>`
   - `get_worker(&self, namespace: &NamespaceId, worker_instance_key: &WorkerInstanceKey) -> Result<Option<WorkerHeartbeat>, HeartbeatStoreError>`
   - `list_workers(&self, namespace: &NamespaceId) -> Result<Vec<WorkerHeartbeat>, HeartbeatStoreError>`
   - `maintain(&self, now: OffsetDateTime, ttl: time::Duration, min_evict_age: time::Duration, max_entries: usize) -> Result<EvictionReport, HeartbeatStoreError>`
3. `insert` SHALL upsert by `(namespace_id, worker_instance_key)`; the newest successful insert wins.
4. `get_worker` and `list_workers` SHALL NOT filter by TTL at read time. Eviction is owned by the runtime maintenance loop.
5. `maintain` SHALL use the caller-supplied `now` for both TTL eviction and capacity eviction eligibility, so tests can exercise maintenance deterministically without reading wall-clock time inside the store.
6. `HeartbeatStoreError` SHALL be a `thiserror`-derived enum with at least `Backend(String)`.
7. `EvictionReport` SHALL include:
   - `ttl_evicted: Vec<(NamespaceId, WorkerInstanceKey)>`
   - `capacity_evicted: Vec<(NamespaceId, WorkerInstanceKey)>`
   - `live: Vec<WorkerHeartbeat>`
   - `namespace_counts: Vec<(NamespaceId, usize)>`
   - `remaining: usize`
8. The maintenance caller SHALL use `live` to record staleness samples, `namespace_counts` to update per-namespace worker-count gauges, and `ttl_evicted` / `capacity_evicted` to set `tokeira_worker_heartbeat_active_state{namespace, worker_instance_key} = 0` for every evicted worker. The store SHALL NOT own metrics.

---

## Feature 2: Decode-Only Edge Translator

### Requirement 2.1: Decode upstream heartbeat into the shared model

**User Story:** As a Tokeira developer, I want edge code to decode upstream heartbeat protos into the compact shared model, so runtime code never depends on Temporal proto types.

#### Acceptance Criteria

1. The translator SHALL live at `crates/tokeira-edge/src/translate/worker_heartbeat/`.
2. The translator SHALL expose `worker_heartbeat_from_proto(namespace_id: NamespaceId, proto: temporal::api::worker::v1::WorkerHeartbeat, now: OffsetDateTime) -> WorkerHeartbeat`.
3. `last_seen` SHALL be set from the server receipt time `now`, not from the worker-authored `heartbeat_time`.
4. `worker_instance_key`, `task_queue`, and `worker_identity` SHALL be copied from the proto input. Empty strings SHALL be preserved rather than rejected for those identity fields.
5. `sdk_name` and `sdk_version` SHALL be copied from the proto input when non-empty. Empty SDK strings SHALL be normalized to `None`; the store treats empty and absent SDK metadata identically.
6. `status` SHALL preserve the upstream worker status value, including shutdown status when supplied by `ShutdownWorker`.
7. `build_id` and `deployment_name` SHALL be extracted from the upstream deployment/version payload when present; otherwise they SHALL be `None`.
8. The decoder SHALL perform no validation beyond what proto decoding already performed. Unknown enums, absent sub-messages, empty strings, and absent timestamps SHALL NOT cause request rejection.
9. The decoder SHALL emit per-heartbeat detail only at `tracing::trace!`, naming `worker_instance_key`. It SHALL NOT emit `debug!`, `info!`, or higher per heartbeat.

### Requirement 2.2: Decode property

**User Story:** As a Tokeira developer, I want decode-side property tests, so future proto bumps or translator edits cannot silently drop in-scope fields.

#### Acceptance Criteria

1. The test suite SHALL include a `proptest` over upstream `WorkerHeartbeat` proto values.
2. The property SHALL assert that every in-scope field in Requirement 2.1 maps into `tokeira-types::WorkerHeartbeat`.
3. The property SHALL assert that absent optional source values decode to `None`, empty `sdk_name` / `sdk_version` decode to `None`, and out-of-scope sub-messages do not cause rejection.
4. Encode-side round-trip testing remains deferred to `worker-deployments`.

---

## Feature 3: Runtime In-Memory Store

### Requirement 3.1: Implement `InMemoryHeartbeatStore`

**User Story:** As a Tokeira developer, I want a runtime-owned in-memory heartbeat store, so heartbeats are observable without adding persistence or kernel state.

#### Acceptance Criteria

1. `tokeira-runtime` SHALL expose `InMemoryHeartbeatStore` implementing `tokeira_types::HeartbeatStore`.
2. `InMemoryHeartbeatStore::new()` SHALL require no parameters.
3. Writes SHALL be partitioned by the full `(NamespaceId, WorkerInstanceKey)` key. A `DashMap` implementation is preferred because it is already a workspace dependency and handles sharding internally.
4. If a manual bucket implementation is chosen, `bucket_index = hash(namespace_id, worker_instance_key) % BUCKET_COUNT`; hashing by namespace alone is rejected.
5. `list_workers(namespace)` SHALL scan all shards/buckets and filter by namespace. This is acceptable because listing is an operator/query path, not the hot heartbeat path.
6. The runtime SHALL register this store as the default heartbeat backing during runtime/service construction.
7. The server construction path SHALL thread `Arc<dyn HeartbeatStore>` from the runtime-owned store into `WorkflowService` and `WorkflowServiceGrpc`; edge code SHALL depend only on the `tokeira-types` trait, not on `InMemoryHeartbeatStore`.

### Requirement 3.2: Retention and maintenance

**User Story:** As an operator, I want stale heartbeat entries evicted and staleness metrics refreshed, so worker observability remains bounded and useful under churn.

#### Acceptance Criteria

1. The runtime store SHALL hardcode:
   - `DEFAULT_ENTRY_TTL = 24h`
   - `DEFAULT_MIN_EVICT_AGE = 10m`
   - `DEFAULT_MAX_ENTRIES = 1_000_000`
   - `DEFAULT_BUCKETS = 10` when manual buckets are used
   - `DEFAULT_MAINTENANCE_INTERVAL = 10s`
2. No heartbeat retention or maintenance setting SHALL be added to `TokeiraConfig`.
3. On insert, `last_seen` SHALL be set to `max(existing.last_seen, heartbeat.last_seen)` for the same key so backward server-clock jumps do not regress freshness.
4. The runtime SHALL spawn a maintenance task that runs every `DEFAULT_MAINTENANCE_INTERVAL`.
5. Each maintenance pass SHALL call `HeartbeatStore::maintain(now, DEFAULT_ENTRY_TTL, DEFAULT_MIN_EVICT_AGE, DEFAULT_MAX_ENTRIES)` with a single captured `now`.
6. Each maintenance pass SHALL:
   - Record staleness metrics for every live entry as `now - last_seen`.
   - Evict entries older than `now - DEFAULT_ENTRY_TTL`.
   - Apply capacity eviction until the live entry count is at or below `DEFAULT_MAX_ENTRIES`, respecting `DEFAULT_MIN_EVICT_AGE`.
   - Use `EvictionReport::live` to record staleness samples and `EvictionReport::namespace_counts` to update `tokeira_worker_heartbeat_entries_observed{namespace}`.
   - Set `tokeira_worker_heartbeat_active_state{namespace, worker_instance_key} = 0` for every key returned in `ttl_evicted` and `capacity_evicted`.
   - Update active-state and count gauges after eviction.

---

## Feature 4: Handler Migration

### Requirement 4.1: Migrate `record_worker_heartbeat`

**User Story:** As a Tokeira developer, I want `record_worker_heartbeat` to decode and store heartbeat observations while preserving SDK-visible behaviour.

#### Acceptance Criteria

1. `record_worker_heartbeat` SHALL preserve v1.62-sync validation: empty `namespace` returns `Status::invalid_argument("namespace is required")`.
2. For non-empty namespaces, the handler SHALL resolve the namespace to `NamespaceId`, decode every `request.worker_heartbeat` element with `worker_heartbeat_from_proto`, and call `HeartbeatStore::insert` for each decoded heartbeat.
3. Empty heartbeat batches SHALL be accepted and return `Ok(RecordWorkerHeartbeatResponse {})`.
4. Store insertion failure SHALL return `Status::internal(...)`, not `Unimplemented`.
5. Success SHALL return `Ok(Response::new(RecordWorkerHeartbeatResponse {}))`, byte-equivalent to the v1.62-sync no-op response.
6. The handler SHALL emit exactly one `tracing::debug!` line per RPC naming the namespace and heartbeat count. Per-heartbeat detail is trace-only.

### Requirement 4.2: Extend `shutdown_worker`

**User Story:** As an operator, I want final worker heartbeat status to be observable when SDKs piggyback it on `ShutdownWorker`.

#### Acceptance Criteria

1. `shutdown_worker` SHALL preserve existing validation: empty namespace or empty sticky task queue returns `Status::invalid_argument`.
2. If `request.worker_heartbeat` is present, the handler SHALL decode it and call `HeartbeatStore::insert` before performing `broker().deny_worker()`.
3. If final-heartbeat insertion fails, the handler SHALL log `warn!` and continue to `broker().deny_worker()`. Final heartbeat storage is best effort and must not block worker shutdown.
4. If `request.worker_heartbeat` is absent, the handler SHALL perform no heartbeat-store interaction.
5. Success SHALL return `Ok(Response::new(ShutdownWorkerResponse {}))`, byte-equivalent to the pre-spec response.

### Requirement 4.3: SDK-observable parity

**User Story:** As an SDK integrator, I want the heartbeat promotion to be invisible to SDK control flow.

#### Acceptance Criteria

1. `RecordWorkerHeartbeat` and `ShutdownWorker` SHALL NOT return `tonic::Code::Unimplemented` from any path introduced by this spec.
2. `DescribeNamespace` and `GetSystemInfo` SHALL continue advertising `worker_heartbeats: true`.
3. Response proto types and field numbering for `RecordWorkerHeartbeat` and `ShutdownWorker` SHALL NOT change.

---

## Feature 5: Operator Metrics

### Requirement 5.1: Emit supported heartbeat metrics

**User Story:** As an operator, I want heartbeat traffic, active worker counts, and heartbeat staleness visible in Prometheus-compatible metrics.

#### Acceptance Criteria

1. The runtime metrics module SHALL register:
   - `tokeira_worker_heartbeats_accepted_total{namespace, worker_instance_key}` counter.
   - `tokeira_worker_heartbeats_rejected_total{namespace, reason}` counter with finite reasons `invalid_namespace` and `store_error`.
   - `tokeira_worker_heartbeat_entries_observed{namespace}` gauge.
   - `tokeira_worker_heartbeat_entries_total` gauge.
   - `tokeira_worker_heartbeat_active_state{namespace, worker_instance_key}` gauge, where `1` means live in the store and `0` means evicted.
   - `tokeira_worker_last_heartbeat_age_seconds{namespace}` histogram.
2. The staleness histogram SHALL be recorded by the runtime maintenance pass, not by recording `0.0` on heartbeat accept.
3. The staleness histogram buckets SHALL cover the SDK cadence floor through the TTL ceiling, for example `[1, 5, 30, 60, 300, 900, 3600, 14400, 43200, 86400]`.
4. Metrics SHALL use only `namespace`, `worker_instance_key`, and the finite `reason` label where specified. Task queue, deployment, build ID, host, and version labels are deferred to `worker-deployments`.
5. Every successful heartbeat insert SHALL increment `tokeira_worker_heartbeats_accepted_total{namespace, worker_instance_key}` and set `tokeira_worker_heartbeat_active_state{namespace, worker_instance_key}` to `1`, including reinsertion after prior eviction.

### Requirement 5.2: Do not rely on per-series unregister

**User Story:** As an operator, I want metrics semantics that work with the current `metrics` crate and Prometheus exporter.

#### Acceptance Criteria

1. The implementation SHALL NOT attempt to unregister per-worker metric series, drop metric handles as a removal mechanism, or reset cumulative counters to simulate deletion.
2. Eviction SHALL set `tokeira_worker_heartbeat_active_state{namespace, worker_instance_key}` to `0`.
3. Dashboards and alerts SHALL filter on `tokeira_worker_heartbeat_active_state == 1` when live-worker semantics are required.
4. Stale per-worker series may remain in the Prometheus output until process restart or Prometheus-side staleness handling.

---

## Feature 6: Worker Query Backing

### Requirement 6.1: Provide a future `ListWorkers` / `DescribeWorker` data source

**User Story:** As the future `worker-deployments` implementer, I want a stable data-source contract for worker queries.

#### Acceptance Criteria

1. Future query code SHALL read through `tokeira_types::HeartbeatStore`.
2. `list_workers(namespace)` SHALL return `Vec<WorkerHeartbeat>`.
3. `get_worker(namespace, worker_instance_key)` SHALL return `Option<WorkerHeartbeat>`.
4. No `tokeira-projection -> tokeira-runtime` dependency SHALL be introduced by this spec.
5. No separate materialized projection state SHALL be maintained; read-after-write and eviction propagation come from reading the runtime store directly through the trait.

---

## Feature 7: Surface Audit

### Requirement 7.1: Amend v1.62-sync audit rows

**User Story:** As a reviewer, I want the prior Surface_Audit to reflect that `RecordWorkerHeartbeat` is no longer an accept-and-discard no-op.

#### Acceptance Criteria

1. The `WorkflowService.RecordWorkerHeartbeat` row in `.kiro/specs/temporal-api-v1.62-sync/design.md` SHALL be amended from `Classification_NoOp` to an observation-backed implementation owned by `worker-heartbeat-observability`.
2. The implementation notes SHALL state: "accept, decode compact heartbeat model, insert into `HeartbeatStore`, emit metrics".
3. Full-fidelity worker sub-message encode/round-trip rows remain deferred to `worker-deployments`; this spec decodes only the compact fields it stores.
4. A structural test SHALL fail if the amended `RecordWorkerHeartbeat` row is reverted.

---

## Feature 8: Correctness Properties

### Requirement 8.1: Kernel purity

1. `tokeira-kernel` SHALL gain no dependency and no transition variant from this spec.
2. A test SHALL assert that the kernel dependency list and transition module list are unchanged for this spec.

### Requirement 8.2: Store determinism

1. Given the same sequence of `insert` calls and maintenance passes, two independent `InMemoryHeartbeatStore` instances SHALL return equal `list_workers(namespace)` outputs after sorting by `(worker_instance_key, last_seen)`.
2. Repeated inserts for the same key SHALL be last-write-wins, with monotonic `last_seen`.

### Requirement 8.3: Handler parity

1. Tests SHALL cover `record_worker_heartbeat` with realistic SDK-shaped payloads, empty batches, missing sub-messages, empty strings, and store errors.
2. Tests SHALL cover `shutdown_worker` with and without `worker_heartbeat`, including the best-effort store-error path.
3. All handler tests SHALL assert that introduced error paths are never `Unimplemented`.
