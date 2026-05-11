# Requirements Document: Worker Heartbeat Observability

## Introduction

`temporal-api-v1.62-sync` classifies `RecordWorkerHeartbeat` as `No-op` and lands an accept-and-discard handler: the RPC decodes an upstream `Vec<temporal::api::worker::v1::WorkerHeartbeat>`, validates that the request's namespace is non-empty, emits one `tracing::debug!` line per call, returns `Ok(RecordWorkerHeartbeatResponse {})`, and throws the payload away. The rationale comment on that handler names `worker-heartbeat-observability` as the spec that will promote the no-op to real observability.

This spec is that promotion. It decodes the `WorkerHeartbeat` payload into Edge DTOs, stores heartbeat records in a runtime-owned in-memory registry the projection layer can observe, exposes worker liveness as a first-class operator observable (metrics, projection query, `ListWorkers` response backing), and keeps the SDK-observable surface byte-identical to what v1.62-sync established. The registry is intentionally in-memory — it matches upstream Temporal's architecture, where heartbeats are observations rather than correctness state.

The spec does NOT change the public `RecordWorkerHeartbeat` RPC signature (that stayed put in v1.62-sync and stays put here), does NOT implement the `ListWorkers` RPC handler (deferred to `worker-deployments`; the projection this spec builds is the data source the future `ListWorkers` handler will read), and does NOT introduce any operator-configurable retention knob — the retention constants mirror upstream Temporal verbatim.

### SDK behaviour reference

This spec's requirements are grounded in the behaviour the Rust SDK (`sdk-core`) and upstream Temporal server exhibit on the `RecordWorkerHeartbeat` path. These are the five load-bearing observations the implementation must honour; each requirement below cites back to this section rather than restating the invariant.

1. **`DescribeNamespace` capability gate.** The SDK's `SharedNamespaceWorker` (`sdk-core/crates/sdk-core/src/worker/heartbeat.rs:69–85`) calls `DescribeNamespace` once on startup. If `namespace_info.capabilities.worker_heartbeats != Some(true)` — including `false`, `None`, or an absent `capabilities` — the shared namespace worker shuts itself down and never sends heartbeats for any worker in that namespace. This is the `v0.4_Liveness_Invariant`; v1.62-sync already established `worker_heartbeats: true` on the advertisement, and this spec preserves it verbatim.
2. **`Unimplemented` kills the heartbeater.** If `RecordWorkerHeartbeat` returns `tonic::Code::Unimplemented` (`sdk-core/crates/sdk-core/src/worker/heartbeat.rs:106–110`), the shared namespace worker shuts down. Any other error is logged and the ticker continues. Our handler must not return `Unimplemented`.
3. **Cadence is SDK-driven, 1–60s, default 60s.** `sdk-core/crates/sdk-core/src/lib.rs:185–201` validates `heartbeat_interval` as `1s ≤ interval ≤ 60s` with a builder-time default of 60s. The server imposes no cadence validation of its own — upstream Temporal's frontend and matching handlers do zero payload validation beyond the per-namespace `WorkerHeartbeatsEnabled` feature flag and namespace resolution (`temporal/service/frontend/workflow_handler.go:5988–6011`, `temporal/service/matching/handler.go:550–557`). Our handler must match this permissiveness.
4. **Heartbeats batch per tick, per namespace.** The shared namespace worker holds a `Uuid → HeartbeatFn` map keyed by `worker_instance_key` (`sdk-core/crates/sdk-core/src/worker/heartbeat.rs:22–26, 88–108`). On each tick it collects callbacks from every registered worker in the namespace and sends one `RecordWorkerHeartbeat` RPC carrying `Vec<WorkerHeartbeat>`. Our handler must accept batched heartbeats and process every element.
5. **Final heartbeat rides on `ShutdownWorker`.** `sdk-core/crates/sdk-core/src/worker/mod.rs:950–985` captures the last heartbeat via the callback and piggybacks it inside `ShutdownWorkerRequest.worker_heartbeat`. Upstream Temporal's frontend handler routes this final heartbeat through `matching.RecordWorkerHeartbeat` so the registry observes the `WORKER_STATUS_SHUTDOWN` transition (`temporal/service/frontend/workflow_handler.go:2720–2762`). Our `shutdown_worker` handler must do the same — see Feature 3.

### What this spec delivers

- A `WorkerHeartbeat` Edge DTO family in `crates/tokeira-edge/src/translate/` that mirrors the v1.62 upstream sub-messages: `WorkerHeartbeat`, `WorkerPollerInfo`, `WorkerSlotsInfo`, `WorkerHostInfo`, `PluginInfo`, and `StorageDriverInfo`. The v1.62-sync Surface_Audit rows for these messages move from `Classification_NoOp` with `compile-only; no DTO/translator work` disposition to `Classification_WireThrough`.
- A **decode-only** translator layer. Encode-to-proto is not needed by this spec — the `ListWorkers` / `DescribeWorker` response path that would serialise is implemented by `worker-deployments`, and that spec owns the encode translator.
- A `HeartbeatStore` trait in `tokeira-runtime` with a default in-memory backing, keyed by `(NamespaceId, WorkerInstanceKey)` and carrying the decoded `WorkerHeartbeat` Edge DTO directly. Retention mirrors upstream Temporal's `service/matching/workers/registry_impl.go` verbatim: 24-hour per-entry TTL, 10-minute minimum eviction age, 1,000,000 global entry cap, 1-hour background eviction sweep, 10-bucket keyspace partitioning for lock contention. All retention constants are hardcoded; no `TokeiraConfig` surface is introduced. The pattern matches `ScheduleStore` / `TaskQueueConfigStore` / `VersioningRuleStore` already established by v1.62-sync.
- A migrated `record_worker_heartbeat` handler in `crates/tokeira-edge/src/grpc/workflow_service.rs` that decodes the payload, routes it to the `HeartbeatStore`, and returns `Ok` with byte-identical RPC response framing to the v1.62-sync no-op so no SDK observes a behavioural change.
- An extended `shutdown_worker` handler in the same file that, when `request.worker_heartbeat` is present, routes it through `HeartbeatStore::record` before performing its existing `broker().deny_worker()` work. Matches upstream Temporal's frontend-to-matching routing pattern and captures the `WORKER_STATUS_SHUTDOWN` transition in the projection.
- A small, registry-bounded set of operator-facing Prometheus-style metrics (heartbeats-accepted-total, workers-observed, workers-total, time-since-last-heartbeat distribution) routed through the existing `tokeira-runtime` metrics abstraction. Metric label cardinality is bounded by the live `HeartbeatStore` registry: labels disappear on eviction, so the 1,000,000 global entry cap is also the global label ceiling.
- A worker-identity projection in `tokeira-projection` that indexes `(NamespaceId, WorkerInstanceKey) → WorkerHeartbeat`, queryable by the future `ListWorkers` handler the `worker-deployments` spec will implement. The projection is a thin read-through over the `HeartbeatStore` — it carries the full decoded DTO, not a lossy summary.
- A Surface_Audit amendment reclassifying `WorkerHeartbeat` plus its six sub-messages from `Classification_NoOp` to `Classification_WireThrough`, and the `RecordWorkerHeartbeat` RPC row likewise.

### What this spec explicitly defers

- **`ListWorkers` RPC handler implementation** — the Worker Deployments RPC that would expose this spec's projection to SDK and UI clients remains stubbed as `Status::unimplemented(...)` per the v1.62-sync decision. This spec makes the data available; `worker-deployments` wires the RPC up.
- **`DescribeWorker` RPC handler implementation** — same reasoning as `ListWorkers`. Deferred to `worker-deployments`.
- **Encode translator and round-trip property for the heartbeat DTO family** — the encode path is needed only by a handler that returns `WorkerHeartbeat` over the wire (`ListWorkers`, `DescribeWorker`), and those handlers are deferred. `worker-deployments` will own both the encode translator and its round-trip property. This spec ships decode-only.
- **Query filtering on `Worker_Projection`** — upstream Temporal's `ListWorkers` accepts a SQL-style query filter (`temporal/service/matching/workers/worker_query_engine.go`). The query parser is a Worker Deployments feature; the projection API shipped here is unfiltered `list_workers(namespace)`, and a filtered variant will be added additively by `worker-deployments` without breaking this spec's API.
- **New kernel transition variants for worker liveness** — heartbeats are observations, not state-changing transitions. Kernel purity (Rule 2 of `tokeira/AGENTS.md`) is preserved.
- **Per-task-queue, per-deployment, or per-build-id metric labels** — metric labels are bounded to `(namespace, worker_instance_key)` only. Those additional dimensions belong to `worker-deployments`.
- **SDK-side versioning of worker identity** — this spec treats `WorkerInstanceKey` as an opaque identifier sourced from `WorkerHeartbeat.worker_instance_key`. Normalisation or identity-merging across heartbeats is out of scope.
- **Heartbeat-driven admission control** — this spec observes worker liveness. It does not use worker liveness to drive admission, throttling, or dispatch decisions.
- **Historical heartbeat archival or query** — the projection carries the latest `WorkerHeartbeat` per `WorkerInstanceKey` only. Time-series heartbeat history is deferred.

Not deferred, and not a gap waiting to be filled: **DSQL-backed heartbeat persistence**. Upstream Temporal's worker registry is 100% in-memory by design (`temporal/service/matching/workers/registry_impl.go` imports no persistence layer). Heartbeats are observations, not correctness state — loss on process restart is acceptable and matches upstream. Tokeira matches upstream by design, not by deferral.

### Cross-references

- [`temporal-api-v1.62-sync`](../temporal-api-v1.62-sync/requirements.md): lands the no-op `record_worker_heartbeat` handler this spec migrates, establishes the Edge DTO layer and `dispatch_rpc` pattern this spec extends, and writes the Surface_Audit this spec amends. This spec's implementation is strictly downstream of v1.62-sync landing.
- [`worker-deployments`](../worker-deployments/requirements.md): consumes the `Worker_Projection` this spec creates, owns the `WorkerHeartbeat` encode translator, and implements `ListWorkers` / `DescribeWorker` as thin reads against the projection. The data-shape contract is declared in Feature 8.
- [`pipeline-foundation`](../pipeline-foundation/requirements.md): no CI infrastructure changes are required by this spec. Property tests introduced here run under the standard `cargo test --workspace` gate.
- [`tkr-cli`](../tkr-cli/requirements.md): no new `tkr` subcommand lands from this spec.
- `temporal-compatibility`: explicitly NOT consumed. The `worker_heartbeats` capability flag is a local edge-level constant established by v1.62-sync; this spec keeps it at `true` without routing through any structural feature matrix, compatibility digest, or `tkr compat` surface.

## Glossary

- **WorkerHeartbeat_Upstream**: The upstream protobuf message `temporal.api.worker.v1.WorkerHeartbeat`, vendored at `v1.62.11` by `temporal-api-v1.62-sync` under `proto/upstream/temporal/api/worker/v1/message.proto`.
- **WorkerHeartbeat_SubMessages**: The six sub-messages that accompany `WorkerHeartbeat` on the wire: `WorkerPollerInfo`, `WorkerSlotsInfo`, `WorkerHostInfo`, `PluginInfo`, `StorageDriverInfo`, and `WorkerHeartbeat` itself. The v1.62-sync Surface_Audit counts seven rows total for the family.
- **Edge_DTO_Family**: The set of wire-agnostic DTO structs this spec adds to `crates/tokeira-edge/src/translate/` mirroring `WorkerHeartbeat_Upstream` and `WorkerHeartbeat_SubMessages`.
- **WorkerInstanceKey**: The opaque identifier that uniquely identifies one worker process instance across its heartbeat stream. Sourced from `WorkerHeartbeat.worker_instance_key`. Treated as an opaque `String` — no parsing, no normalisation, no inferred structure.
- **HeartbeatStore**: The trait this spec adds to `tokeira-runtime`, with a default in-memory backing, storing `(NamespaceId, WorkerInstanceKey) → WorkerHeartbeat`. The retention semantics mirror upstream Temporal's matching-service registry.
- **Upstream_Retention_Constants**: The five hardcoded constants in `temporal/service/matching/workers/registry_impl.go:17–21`: TTL 24h, min-evict-age 10m, max-entries 1,000,000, eviction-interval 1h, buckets 10. This spec adopts all five verbatim.
- **Worker_Projection**: The read model this spec adds to `tokeira-projection` indexing `(NamespaceId, WorkerInstanceKey) → WorkerHeartbeat`. A thin read-through over the `HeartbeatStore`. Returns the full decoded DTO.
- **Heartbeat_Metrics**: The set of operator-facing Prometheus-style metrics this spec introduces. Cardinality is bounded by the live registry — labels disappear when entries are evicted. Labels use `namespace` and `worker_instance_key` only.
- **v1.62_Surface_Audit**: The Surface_Audit table produced by `temporal-api-v1.62-sync` in `.kiro/specs/temporal-api-v1.62-sync/design.md`. This spec amends specific rows per Feature 7.
- **Handler_Behaviour_Parity**: The property that an SDK worker connected to a pre-spec `tokeirad` (v1.62-sync only) and a post-spec `tokeirad` observes identical **SDK-observable** surface: `RecordWorkerHeartbeat` response bytes, `ShutdownWorker` response bytes, RPC status codes (no new `Unimplemented`), and `DescribeNamespace` / `GetSystemInfo` capability bytes. Server-side operator observability — projection contents, metrics, structured logs — legitimately differs between pre-spec and post-spec, and that difference is the point of the spec.
- **Kernel_Purity_Rule**: Rule 2 of `tokeira/AGENTS.md`: the `tokeira-kernel` crate has no I/O, no async, no storage, no metrics, no network. All `HeartbeatStore` and projection work runs in `tokeira-runtime` and `tokeira-projection`; the kernel gains no new transition variants.

## Requirements

---

## Feature 1: Heartbeat DTO Family (decode-only)

### Requirement 1.1: Introduce the `WorkerHeartbeat` Edge DTO family

**User Story:** As a Tokeira developer, I want a wire-agnostic Edge DTO family mirroring the upstream `WorkerHeartbeat` message and its six sub-messages, so that the runtime and projection layers consume decoded heartbeats rather than raw proto types and so the DTO boundary stays stable across upstream proto minor versions.

#### Acceptance Criteria

1. THE Edge_DTO_Family SHALL live in a submodule of `crates/tokeira-edge/src/translate/` named `worker_heartbeat`. THE module layout SHALL match neighbouring DTO families such as `schedule` and `versioning_rule`.
2. THE Edge_DTO_Family SHALL contain one Rust struct per WorkerHeartbeat_Upstream sub-message: `WorkerHeartbeat`, `WorkerPollerInfo`, `WorkerSlotsInfo`, `WorkerHostInfo`, `PluginInfo`, and `StorageDriverInfo`. Naming SHALL match the upstream proto message names verbatim.
3. EACH Edge_DTO struct SHALL derive `Debug`, `Clone`, `PartialEq`, `Eq`, `Serialize`, and `Deserialize`, matching the convention used by peer DTO families.
4. EACH field on WorkerHeartbeat_Upstream and WorkerHeartbeat_SubMessages SHALL have a corresponding field on the Edge_DTO struct with a Rust-idiomatic name (`snake_case` names, `Option<T>` for proto-optional scalar wrappers, `Vec<T>` for `repeated` fields, `tokeira_types::Duration` for `google.protobuf.Duration`, the existing `tokeira_types` timestamp newtype for `google.protobuf.Timestamp`).
5. THE DTOs SHALL NOT carry proto-layer types (`prost_types::Timestamp`, `prost_types::Duration`). Time types SHALL use the existing `tokeira_types` newtypes the neighbouring DTOs already use.
6. WHERE the upstream message contains a submessage field (for example `WorkerHeartbeat.host_info: WorkerHostInfo`), THE Edge_DTO SHALL carry `Option<WorkerHostInfo>` because the upstream proto layer treats sub-messages as optional on the wire — absence on the wire SHALL translate to `None` on the DTO.
7. IF a future v1.63+ upstream bump adds a field to any WorkerHeartbeat_Upstream or WorkerHeartbeat_SubMessages struct, THEN that field's handling SHALL be governed by whichever spec performs the bump, not by this spec. This spec's DTOs reflect the v1.62.11 surface only.

### Requirement 1.2: Decode translator from upstream proto to Edge DTO

**User Story:** As a Tokeira developer, I want a translator function that decodes an upstream `WorkerHeartbeat` proto into the Edge DTO, so that the `record_worker_heartbeat` and `shutdown_worker` handlers consume a typed DTO rather than the raw proto.

#### Acceptance Criteria

1. THE translator module SHALL expose a function `from_proto::worker_heartbeat_from_proto(proto: temporal::api::worker::v1::WorkerHeartbeat) -> WorkerHeartbeat` returning the Edge_DTO variant.
2. THE translator module SHALL expose sibling functions for each of the six sub-messages (`worker_poller_info_from_proto`, `worker_slots_info_from_proto`, `worker_host_info_from_proto`, `plugin_info_from_proto`, `storage_driver_info_from_proto`).
3. WHERE the upstream proto field is `repeated`, THE translator SHALL call the appropriate sub-message translator for each element, preserving wire order.
4. WHERE the upstream proto field is `optional`, THE translator SHALL map absence to `None` and presence to `Some(value)` without silently substituting defaults.
5. THE decoder SHALL match upstream Temporal's permissiveness (see SDK behaviour reference item 3): NO payload validation beyond what the proto runtime already enforces. In particular, the decoder SHALL NOT reject heartbeats based on unknown enum values, out-of-range timestamps, empty strings, empty sub-messages, or empty repeated fields. Unknown enum values SHALL decode to the Edge_DTO's `Unspecified` variant (or equivalent). Out-of-range timestamps SHALL decode to whatever the `tokeira_types` timestamp newtype permits; the decoder SHALL NOT surface a parse error to the caller.
6. THE decoder SHALL emit at most one `tracing::debug!` log line per decoded `WorkerHeartbeat`, naming `worker_instance_key`. No `info` or higher level log SHALL be emitted per-heartbeat — SDK cadence at 1s would flood operator logs (see SDK behaviour reference item 3).

### Requirement 1.3: Decode property — round-trip through an encoder is out of scope for this spec

**User Story:** As a Tokeira developer, I want a property test asserting that the decoder is idempotent and structurally lossless against the v1.62 proto surface, so that future upstream bumps are caught structurally even without an encode-side closure.

#### Acceptance Criteria

1. THE test suite SHALL include a `proptest`-driven structural property over `temporal::api::worker::v1::WorkerHeartbeat` that generates arbitrary valid proto instances via `proptest` strategies and asserts that decoding them produces a `WorkerHeartbeat` Edge DTO whose field-by-field values mirror the proto's field-by-field values under the translation rules of Requirement 1.2.
2. THE property test SHALL assert structural properties only (no round-trip through an encode path, which does not exist in this spec). Example invariants: every `repeated` field on the proto yields a `Vec` of the same length on the DTO; every sub-message present on the proto yields `Some` on the DTO; every sub-message absent on the proto yields `None` on the DTO.
3. THE test SHALL live in `crates/tokeira-edge/src/translate/worker_heartbeat/` (co-located with the module).
4. THE `worker-deployments` spec SHALL own the encode translator and its full decode-encode round-trip property; the structural property shipped here is the decode-side half.

---

## Feature 2: In-Memory HeartbeatStore

### Requirement 2.1: Define the `HeartbeatStore` trait

**User Story:** As a Tokeira developer, I want a `HeartbeatStore` trait in `tokeira-runtime` with a clear contract for writes, reads, and eviction, so that the storage backing is pluggable and so the handler and projection layers depend on the trait not the default implementation.

#### Acceptance Criteria

1. THE `HeartbeatStore` trait SHALL live in `crates/tokeira-runtime/src/heartbeat/` (a new module), following the submodule convention of peer stores such as `schedule` and `task_queue_config`.
2. THE trait SHALL declare at minimum:
   - `record(&self, namespace: &NamespaceId, heartbeats: Vec<WorkerHeartbeat>) -> Result<(), HeartbeatStoreError>` — upserts a batch keyed by `(NamespaceId, WorkerInstanceKey)`.
   - `get(&self, namespace: &NamespaceId, worker_instance_key: &str) -> Result<Option<WorkerHeartbeat>, HeartbeatStoreError>` — returns the latest `WorkerHeartbeat` for one worker identity, or `None` if the key is not present. Matches upstream `DescribeWorker` semantics.
   - `list(&self, namespace: &NamespaceId) -> Result<Vec<WorkerHeartbeat>, HeartbeatStoreError>` — returns every stored `WorkerHeartbeat` in the namespace. Ordering is unspecified. Matches upstream `ListWorkers` semantics.
   - `evict(&self) -> Result<EvictionReport, HeartbeatStoreError>` — runs one TTL pass followed by one capacity pass per Requirement 2.3. Exposed on the trait so tests can drive eviction deterministically.
3. THE `HeartbeatStoreError` type SHALL be a `thiserror`-derived enum carrying at minimum a `Backend(String)` variant; `anyhow::Error` at the store boundary is explicitly rejected per the error-handling convention established by v1.62-sync Requirement 4.6.2.
4. THE trait SHALL be `Send + Sync + 'static`.
5. WHERE a method takes `&self` (not `&mut self`), THE default implementation SHALL achieve interior mutability via per-bucket `std::sync::Mutex`, matching upstream Temporal's `bucket.mu` pattern and avoiding async-lock overhead on a hot path that performs no I/O.

### Requirement 2.2: Default in-memory backing

**User Story:** As a Tokeira developer, I want a default in-memory `HeartbeatStore` implementation requiring no external dependency, so that the feature works out of the box and mirrors upstream Temporal's in-process registry footprint.

#### Acceptance Criteria

1. THE `tokeira-runtime` crate SHALL expose a public type `InMemoryHeartbeatStore` implementing `HeartbeatStore`.
2. THE `InMemoryHeartbeatStore` SHALL be constructible via `InMemoryHeartbeatStore::new()` with zero parameters. Retention constants enumerated in Requirement 2.3 are hardcoded inside the module; there is no config struct.
3. THE `InMemoryHeartbeatStore` SHALL partition storage across the bucket count from Requirement 2.3. Each bucket SHALL hold its own entry map keyed by `(NamespaceId, WorkerInstanceKey)` and carrying the decoded `WorkerHeartbeat` Edge DTO plus a server-side `last_seen` timestamp for TTL and LRU accounting. An LRU-ordered linked list per bucket supports O(1) capacity eviction, mirroring upstream Temporal's `bucket` struct. Exact field layout is a design-phase decision provided behaviour matches Requirement 2.3.
4. WHEN `record` is called with multiple heartbeats that share a `(NamespaceId, WorkerInstanceKey)` key, THE latest heartbeat by input-vector order SHALL win. This matches upstream Temporal's loop-and-overwrite semantics.
5. THE `InMemoryHeartbeatStore` SHALL register itself as the default backing for `tokeira-runtime` initialisation, consistent with `TaskQueueConfigStore` in v1.62-sync.

### Requirement 2.3: Retention semantics (TTL + capacity eviction)

**User Story:** As an operator, I want expired heartbeat records evicted automatically and memory usage bounded under worker churn, so that the store's footprint stays predictable on clusters with many short-lived workers and so behaviour matches upstream Temporal dashboards.

#### Acceptance Criteria

1. THE retention constants SHALL match `Upstream_Retention_Constants` verbatim: TTL 24 hours (`defaultEntryTTL`), minimum eviction age 10 minutes (`defaultMinEvictAge`), global entry cap 1,000,000 (`defaultMaxEntries`), eviction sweep interval 1 hour (`defaultEvictionInterval`), bucket shard count 10 (`defaultBuckets`). All five SHALL be hardcoded constants in the default in-memory store; NONE SHALL be surfaced through `TokeiraConfig`.
2. WHEN a heartbeat for a `(NamespaceId, WorkerInstanceKey)` key is recorded, THE entry's internal `last_seen` timestamp SHALL be set to the server clock at record time. Subsequent heartbeats for the same key SHALL refresh `last_seen` to `max(current_last_seen, new_record_time)` — out-of-order delivery with a backward-jumped server clock SHALL NOT regress `last_seen`.
3. `HeartbeatStore::get` AND `HeartbeatStore::list` SHALL NOT filter by TTL at read time. Reads return whatever entries are currently held in the backing map. TTL filtering is a background-sweep concern only. Upstream Temporal's `ListWorkers` and `DescribeWorker` likewise do not filter at read time; the 24-hour TTL ensures any stale-but-not-yet-evicted entry is at most one sweep interval behind.
4. THE `tokeira-runtime` initialisation SHALL spawn a background task that invokes `HeartbeatStore::evict` every `defaultEvictionInterval` (1 hour). `evict` SHALL perform two passes in order: first a TTL pass that removes every entry whose `last_seen` is older than `now - defaultEntryTTL`, then a capacity pass that removes the oldest-by-`last_seen` entries (respecting a floor of `defaultMinEvictAge` below which entries are never capacity-evicted) until the global entry count is at or below `defaultMaxEntries`.
5. THE `defaultBuckets` shards SHALL partition the keyspace by hashing `NamespaceId`, so that concurrent writers on different namespaces contend on different bucket locks. The 10-bucket constant is a lock-contention knob, not a semantic one — it does not affect observable retention behaviour.

### Requirement 2.4: No operator-facing retention configuration

**User Story:** As a Tokeira maintainer, I want the heartbeat retention constants kept off the operator-configurable surface, so that the close-to-zero-config principle of `tokeira/AGENTS.md` is respected and so operators cannot accidentally shrink or inflate the retention window in a way that breaks parity with upstream Temporal dashboards.

#### Acceptance Criteria

1. THE `TokeiraConfig` struct in `tokeira-config` SHALL NOT gain a field for heartbeat TTL, minimum eviction age, global entry cap, eviction sweep interval, or bucket count.
2. WHERE future operational need emerges to tune any of these constants, THE change SHALL go through an explicit spec that names the scenario and the observed problem the tunability is solving. Adding a knob "because it might be useful" is explicitly rejected.
3. ANY deviation from the `Upstream_Retention_Constants` values in the Tokeira implementation SHALL be flagged in the design document with rationale; silent divergence is a bug.

---

## Feature 3: Handler Migration

### Requirement 3.1: Migrate `record_worker_heartbeat` from no-op to store-routing handler

**User Story:** As a Tokeira developer, I want the `record_worker_heartbeat` handler migrated from its v1.62-sync no-op implementation to a handler that decodes the payload and routes it to the `HeartbeatStore`, so that the RPC finally observes the payload rather than discarding it while preserving SDK-observable behaviour.

#### Acceptance Criteria

1. THE handler `record_worker_heartbeat` in `crates/tokeira-edge/src/grpc/workflow_service.rs` SHALL decode every element of `request.worker_heartbeat: Vec<temporal::api::worker::v1::WorkerHeartbeat>` via `from_proto::worker_heartbeat_from_proto` into `Vec<WorkerHeartbeat>` (Edge_DTO) per Feature 1.
2. THE handler SHALL invoke `HeartbeatStore::record(namespace, heartbeats)` with the decoded batch. IF `HeartbeatStore::record` returns `Err`, THEN the handler SHALL return `Status::internal(<error>)` — the SDK treats non-`Unimplemented` errors as transient and retries at the next cadence tick (see SDK behaviour reference item 2).
3. THE handler SHALL return `Ok(Response::new(RecordWorkerHeartbeatResponse {}))` on success. THE response payload SHALL be byte-equivalent to the v1.62-sync no-op response to preserve Handler_Behaviour_Parity.
4. THE handler SHALL preserve the v1.62-sync validation behaviour verbatim: IF `request.namespace` is empty, THEN the handler SHALL return `Status::invalid_argument("namespace is required")`. This is a deliberate Tokeira tightening over upstream — upstream returns `NotFound` from the namespace registry lookup; Tokeira returns `InvalidArgument` matching `shutdown_worker` convention (see v1.62-sync Requirement 3.4.5).
5. THE handler SHALL NOT perform any other payload validation beyond the namespace non-empty check. `worker_instance_key` may be empty, `worker_heartbeat` may be an empty `Vec`, sub-messages may be `None`, timestamps may be absent or out-of-range, enums may be unknown — all SHALL be accepted and passed through to the store. Matches upstream Temporal permissiveness (see SDK behaviour reference item 3).
6. THE handler SHALL NOT be reachable via a `tonic::Code::Unimplemented` return path under any code path this spec introduces. Returning `Unimplemented` would trigger SDK `SharedNamespaceWorker` shutdown (see SDK behaviour reference item 2).
7. THE handler SHALL emit exactly one `tracing::debug!` line per call naming the RPC and `heartbeat_count`. Per-heartbeat detail SHALL be at `tracing::trace!` only. No `info` or higher level log SHALL be emitted per-call — SDK cadence at 1s with many workers per namespace would flood operator logs (see SDK behaviour reference item 3).
8. THE rationale comment previously anchored on the no-op handler (see v1.62-sync Requirement 3.4.4 and 3.3.3) SHALL be removed — the handler no longer forwards to a follow-up spec because this spec is that follow-up. A replacement comment SHALL state the current spec name (`worker-heartbeat-observability`) as the owner of the handler's behaviour.

### Requirement 3.2: Extend `shutdown_worker` to route the final heartbeat

**User Story:** As an operator, I want the `WORKER_STATUS_SHUTDOWN` status transition observable in the projection and metrics, so that a gracefully-shutdown worker is cleanly reflected in worker observability rather than disappearing silently at the next TTL tick.

#### Acceptance Criteria

1. THE existing `shutdown_worker` handler in `crates/tokeira-edge/src/grpc/workflow_service.rs` SHALL be extended to inspect `request.worker_heartbeat: Option<temporal::api::worker::v1::WorkerHeartbeat>`. WHEN the field is `Some(heartbeat)`, THE handler SHALL decode it via `from_proto::worker_heartbeat_from_proto` and call `HeartbeatStore::record(&namespace_id, vec![heartbeat])` before performing its existing `broker().deny_worker()` work.
2. THE order SHALL be record-before-deny so that the final heartbeat's `WORKER_STATUS_SHUTTING_DOWN` / `WORKER_STATUS_SHUTDOWN` status is observable in the projection at the moment the poller is denied. This matches upstream Temporal's frontend-to-matching routing at `temporal/service/frontend/workflow_handler.go:2720–2762`.
3. IF `HeartbeatStore::record` returns `Err`, THEN the handler SHALL log a `tracing::warn!` line naming the worker_instance_key and the error, and SHALL continue to perform `broker().deny_worker()`. The final-heartbeat routing is best-effort; a storage failure SHALL NOT block the poller-deny path. Matches upstream behaviour at `temporal/service/frontend/workflow_handler.go:2744–2748`.
4. THE existing `shutdown_worker` validation (empty namespace or empty sticky_task_queue → `Status::invalid_argument`) SHALL be preserved. The response payload SHALL remain byte-equivalent to the pre-spec response to preserve Handler_Behaviour_Parity.
5. WHEN `request.worker_heartbeat` is `None`, THE handler SHALL behave exactly as the pre-spec handler — no projection update, no metric emission. Not every SDK language populates the final-heartbeat field; absence is normal.

### Requirement 3.3: SDK-observable behaviour parity

**User Story:** As an SDK integrator, I want an SDK worker connected to a pre-spec `tokeirad` (v1.62-sync only) and a post-spec `tokeirad` to observe identical SDK-observable surface, so that this spec is transparent to the SDK and therefore to any workflow whose correctness depends on SDK behaviour.

#### Acceptance Criteria

1. WHEN an SDK worker calls `RecordWorkerHeartbeat` against a post-spec `tokeirad`, THE response wire bytes SHALL be byte-equivalent to those returned by a v1.62-sync-only `tokeirad` for the same request. Because `RecordWorkerHeartbeatResponse` is an empty proto message, response bytes are always zero-length regardless of handler internals; this requirement is structural and verified by a unit test asserting the handler's success return constructs `RecordWorkerHeartbeatResponse {}` (not a decorated variant).
2. WHEN an SDK worker calls `ShutdownWorker` against a post-spec `tokeirad`, THE response wire bytes SHALL be byte-equivalent to the pre-spec response, likewise verified structurally (`ShutdownWorkerResponse` is an empty proto message).
3. WHEN an SDK worker calls `GetSystemInfo` or `DescribeNamespace` against a post-spec `tokeirad`, THE `worker_heartbeats` capability bytes SHALL be byte-equivalent to the pre-spec response — this spec does not change the advertised value (`true` was established by v1.62-sync). Preservation of this invariant is required to keep the SDK's `SharedNamespaceWorker` alive (see SDK behaviour reference item 1).
4. THE handler SHALL NOT return `tonic::Code::Unimplemented` from any code path this spec introduces. A test SHALL assert that the post-spec `record_worker_heartbeat` and `shutdown_worker` handlers, under all documented error conditions (storage failure, empty namespace, empty sticky_task_queue), return either `Ok` or a status code other than `Unimplemented`.
5. Operator-observable surface — `heartbeats_accepted_total` counter increments, `workers_observed` gauge values, projection contents, structured log lines — is explicitly **outside** the behaviour-parity contract. Pre-spec and post-spec `tokeirad` legitimately differ on these dimensions, and that difference is the point of the spec.

---

## Feature 4: Operator Metrics

### Requirement 4.1: Heartbeat-acceptance and worker-observation metrics

**User Story:** As an operator, I want counters for accepted heartbeats, a gauge for currently-observed workers, and a lag histogram, so that heartbeat traffic, worker-population trends, and staleness are visible in dashboards.

#### Acceptance Criteria

1. THE `tokeira-runtime` metrics module SHALL register the following counters as part of `HeartbeatStore` initialisation:
   - `heartbeats_accepted_total{namespace, worker_instance_key}` — cumulative count of heartbeats accepted for each worker identity. Incremented per-element during `HeartbeatStore::record`.
   - `heartbeats_rejected_total{namespace, reason}` — cumulative count of rejected heartbeats with a reason label. Reasons SHALL be drawn from a fixed finite set: `store_error` (store returned `Err` from `record`), `invalid_namespace` (empty namespace rejection).
2. THE `tokeira-runtime` metrics module SHALL register the following gauges:
   - `workers_observed{namespace}` — count of entries currently held in the `HeartbeatStore` for the namespace. Updated on each `record` call and after every eviction pass.
   - `workers_total` — count of entries across all namespaces. Tracks the global capacity budget so operators can observe headroom before capacity eviction engages.
3. THE `tokeira-runtime` metrics module SHALL register the following histogram:
   - `seconds_since_last_heartbeat{namespace}` — distribution of `(now - last_seen)` across all stored entries at scrape time. Surfaces the lag distribution operators need to spot stuck workers before TTL eviction removes them.
4. THE histogram buckets for `seconds_since_last_heartbeat` SHALL cover from the SDK cadence floor (1 second, per `sdk-core/crates/sdk-core/src/lib.rs:197`) through the TTL ceiling (86,400 seconds, per Requirement 2.3.1). Approximately logarithmic spacing such as `[1, 5, 30, 60, 300, 900, 3600, 14400, 43200, 86400]` is sufficient; exact boundaries are a design-phase decision.

### Requirement 4.2: Metric cardinality is bounded by the live registry

**User Story:** As an operator, I want heartbeat metric label cardinality bounded by the `HeartbeatStore` contents itself, so that the global 1,000,000-entry store cap is also the global label ceiling and so evicted workers do not leave orphaned metric series.

#### Acceptance Criteria

1. THE metrics enumerated in Requirement 4.1 SHALL use only the following label dimensions: `namespace` and `worker_instance_key`. NO metric SHALL include task-queue, deployment, build-id, host, or version labels. Additional dimensions belong to `worker-deployments`.
2. WHEN the `HeartbeatStore` evicts a `(NamespaceId, WorkerInstanceKey)` entry (TTL pass or capacity pass), THE metrics module's `heartbeats_accepted_total{namespace, worker_instance_key}` and `seconds_since_last_heartbeat{namespace, worker_instance_key}` series for that label combination SHALL be removed. THE store's eviction code path SHALL invoke the metrics unregister path directly — a callback on the `HeartbeatStore` trait is an acceptable implementation choice.
3. THE metrics module SHALL use the `metrics` crate's `Counter::absolute(0)` / reset semantics (or equivalent per the chosen backend) to achieve per-series removal. THE design document SHALL name the concrete approach; the spec mandates a single deterministic behaviour and rejects backend-conditional hedging.
4. THE global label cardinality ceiling for `heartbeats_accepted_total` SHALL equal `defaultMaxEntries` (1,000,000) per Requirement 2.3.1, automatically tracking any future change to that constant without further configuration.

### Requirement 4.3: Metric documentation

**User Story:** As an operator, I want the heartbeat metrics documented with units, label dimensions, and intended use, so that dashboards and alerts cite the intended semantics rather than guessing.

#### Acceptance Criteria

1. EACH metric registered by this spec SHALL have a help string that names the metric, its unit (count, seconds), its label dimensions, and its intended operational use.
2. THE design document SHALL include a metrics reference section listing all metrics introduced by this spec with their help strings verbatim.

---

## Feature 5: Worker Projection

### Requirement 5.1: Introduce the `Worker_Projection` read model

**User Story:** As the future `worker-deployments` spec implementer, I want a worker-identity projection in `tokeira-projection` keyed by `(NamespaceId, WorkerInstanceKey)` that returns `WorkerHeartbeat` directly and matches upstream Temporal's `ListWorkers` / `DescribeWorker` return shapes, so that I can implement those RPCs as thin passthroughs when this spec's projection is in place.

#### Acceptance Criteria

1. THE `tokeira-projection` crate SHALL gain a new projection module (`crates/tokeira-projection/src/worker/` or `crates/tokeira-projection/src/worker.rs`) containing the Worker_Projection implementation.
2. THE projection SHALL expose:
   - `get_worker(namespace: &NamespaceId, worker_instance_key: &str) -> Option<WorkerHeartbeat>` — returns the full stored `WorkerHeartbeat` for one worker identity, or `None` if the key is not present. Matches upstream `DescribeWorker` semantics.
   - `list_workers(namespace: &NamespaceId) -> Vec<WorkerHeartbeat>` — returns every stored `WorkerHeartbeat` in the namespace. Ordering is unspecified; callers sort explicitly if ordering matters. Matches upstream `ListWorkers` semantics.
3. THE projection read API SHALL return `WorkerHeartbeat` Edge DTOs directly. It SHALL NOT introduce a lossy summary type, and it SHALL NOT leak `HeartbeatStore` internal types or `temporal-proto` types.
4. THE projection SHALL be a direct read-through over the `HeartbeatStore`: `Worker_Projection::list_workers(ns)` is a thin adapter over `HeartbeatStore::list(ns)`. No separate projection state is maintained. The read-after-write property is automatic — the projection reads whatever the store most recently accepted. Eviction propagation is automatic for the same reason.
5. THE query-filter surface (SQL-style predicates per upstream Temporal's `worker_query_engine.go`) is explicitly out of scope for this spec; it will be added additively by `worker-deployments`. The API shipped here is unfiltered; future `list_workers_filtered` or `list_workers_paged` variants can be added without breaking compatibility with callers of `list_workers`.

### Requirement 5.2: Projection determinism

**User Story:** As a Tokeira developer, I want the projection derived deterministically from the heartbeat stream, so that replaying the stream produces identical projection state.

#### Acceptance Criteria

1. GIVEN a fixed sequence of `HeartbeatStore::record` calls, THE resulting projection state (`list_workers` output modulo order) SHALL be a pure function of the input sequence. No non-deterministic fields SHALL be inserted by the projection layer on top of what the decoder produced.
2. THE test suite SHALL include a property test asserting that, for two independent `HeartbeatStore` + `Worker_Projection` instances fed the same `(namespace, heartbeats)` sequence, the resulting `list_workers(namespace)` outputs are equal after sorting by `worker_instance_key`. THE test SHALL live in `crates/tokeira-projection/src/worker/` and SHALL use `proptest` strategies.
3. THE projection SHALL NOT emit metrics or logs of its own. Metrics and logging on the heartbeat path are owned by `HeartbeatStore::record` callers (Feature 4); the projection is a read-only view.

---

## Feature 6: Capability Advertisement Preservation

### Requirement 6.1: `worker_heartbeats` capability remains `true`

**User Story:** As an SDK integrator, I want the `DescribeNamespace` and `GetSystemInfo` responses to continue advertising `worker_heartbeats: true`, so that an SDK handshake across v1.62-sync → this spec is transparent and so the SDK's `SharedNamespaceWorker` stays alive (see SDK behaviour reference item 1).

#### Acceptance Criteria

1. THE advertised value of `NamespaceInfo.Capabilities.worker_heartbeats` SHALL remain `true` after this spec lands. No change to `namespace_to_proto` is required beyond ensuring the advertisement still reads from the same source of truth v1.62-sync Requirement 3.3 established.
2. THE advertised value of `GetSystemInfoResponse.Capabilities.worker_heartbeats` SHALL match `NamespaceInfo.Capabilities.worker_heartbeats` consistent with v1.62-sync Requirement 4.1.
3. WHEN an SDK worker calls `DescribeNamespace` against a post-spec `tokeirad`, THE `capabilities.worker_heartbeats` field SHALL be `true` and the SDK's `SharedNamespaceWorker` SHALL NOT shut down at startup — the `v0.4_Liveness_Invariant` established by v1.62-sync is preserved verbatim.
4. THE source of truth for the `worker_heartbeats` capability SHALL remain a local constant in the edge namespace-capabilities assembly path. Whether that constant is later promoted to a structural feature matrix is out of scope for this spec; heartbeat observability correctness does not depend on such a matrix.

### Requirement 6.2: No change to `RecordWorkerHeartbeat` or `ShutdownWorker` wire surface

**User Story:** As a Tokeira developer, I want the public wire surface of `RecordWorkerHeartbeat` and `ShutdownWorker` — proto message types, enum values, field numbering — untouched by this spec, so that the SDK contract stays stable.

#### Acceptance Criteria

1. THE proto definitions under `proto/upstream/temporal/api/` SHALL NOT be edited by this spec. The v1.62-sync resync is the sole editor of that tree.
2. THE generated Rust types for `RecordWorkerHeartbeatRequest`, `RecordWorkerHeartbeatResponse`, `ShutdownWorkerRequest`, and `ShutdownWorkerResponse` SHALL carry the same Rust type signature before and after this spec lands.
3. THE edge-level `WorkflowService` trait method signatures for `record_worker_heartbeat` and `shutdown_worker` SHALL be byte-identical before and after this spec — what changes is the method bodies.

---

## Feature 7: Surface_Audit Amendment

### Requirement 7.1: Reclassify heartbeat entries in the v1.62-sync Surface_Audit

**User Story:** As a Tokeira reviewer, I want the v1.62-sync Surface_Audit entries for `WorkerHeartbeat` and its six sub-messages updated from `No-op` to `Wire through` when this spec lands, so that the canonical audit reflects the current classification.

#### Acceptance Criteria

1. WHEN this spec lands, THE Surface_Audit table in `.kiro/specs/temporal-api-v1.62-sync/design.md` SHALL be amended such that the rows for the following qualified names move from `Classification_NoOp` with disposition `compile-only; no DTO/translator work` to `Classification_WireThrough` with `worker-heartbeat-observability` in the `Target Spec` column:
   - `temporal.api.worker.v1.WorkerHeartbeat`
   - `temporal.api.worker.v1.WorkerPollerInfo`
   - `temporal.api.worker.v1.WorkerSlotsInfo`
   - `temporal.api.worker.v1.WorkerHostInfo`
   - `temporal.api.worker.v1.PluginInfo`
   - `temporal.api.worker.v1.StorageDriverInfo`
2. THE Surface_Audit row for the `RecordWorkerHeartbeat` RPC SHALL be reclassified from `Classification_NoOp` to `Classification_WireThrough`. The handler now decodes the payload, stores it, and exposes it via the projection — it observes the wire payload end-to-end. The empty response body is irrelevant to classification; what matters is whether the request payload is preserved through server state, and it now is. The row's `Implementation Notes` SHALL be updated to "accept, decode, route to `HeartbeatStore`, expose via `Worker_Projection`, emit metrics".
3. THE Implementation & Escalation Matrix (v1.62_Impl_Matrix) in the same design document SHALL be amended in lockstep: the `RecordWorkerHeartbeat` row's `Runtime Impact` column SHALL change from `none` to `new store (HeartbeatStore)` and its `Projection Impact` column from `none` to `new projection (Worker_Projection)`.
4. THE test suite SHALL include a structural assertion that parses the markdown tables in the v1.62-sync design document and fails with a clear diff message if the amended rows are reverted in a future edit.
5. IF the v1.62-sync design document has not yet been written when this spec's implementation lands, THEN this spec's implementation SHALL block until it exists with the rows to amend. The landing dependency on v1.62-sync is hard, not advisory.

### Requirement 7.2: Document the amendment convention for future specs

**User Story:** As a future Tokeira spec author, I want a documented convention for amending a prior spec's Surface_Audit when a follow-up spec promotes a deferred or no-op item to full implementation, so that this spec's pattern is reusable.

#### Acceptance Criteria

1. THE design document for this spec SHALL include a "Surface_Audit Amendment Pattern" section describing the amendment performed here and naming the pattern so future specs (e.g. `worker-deployments`) can reference and reuse it.
2. THE section SHALL state the invariant that the Surface_Audit in a prior spec is the source of truth for current classification, and that follow-up specs amend rather than shadow it — a parallel audit in a new spec is explicitly rejected.
3. THE section SHALL point at Requirement 7.1.4 as the enforcement mechanism.

---

## Feature 8: Cross-References

### Requirement 8.1: Data-shape contract for `worker-deployments`

**User Story:** As the implementer of the future `worker-deployments` spec, I want the projection's return-type contract declared in a stable, inspectable location naming its upstream Temporal reference, so that I can target it when implementing `ListWorkers` / `DescribeWorker` without re-reading this spec.

#### Acceptance Criteria

1. THE design document for this spec SHALL declare the `Worker_Projection` return-type contract (Requirement 5.1) in a dedicated section titled "Projection Contract for `worker-deployments`".
2. THE contract section SHALL state that `list_workers` returns `Vec<WorkerHeartbeat>` and `get_worker` returns `Option<WorkerHeartbeat>`, with `WorkerHeartbeat` being the Edge DTO defined in Feature 1. It SHALL name the upstream Temporal references (`temporal/service/matching/workers/registry.go` and `temporal/service/matching/workers/worker_query_engine.go`) as the behavioural reference.
3. WHEN the `worker-deployments` spec lands, IT SHALL reference this contract section by anchor, SHALL own the encode translator and its full round-trip property, and SHALL NOT duplicate the contract text.

### Requirement 8.2: Explicit non-consumption

**User Story:** As a reviewer, I want explicit "not consumed by this spec" pointers to every adjacent spec this feature might plausibly depend on, so that no implicit dependency is inferred from silence.

#### Acceptance Criteria

1. THE Introduction's Cross-references section SHALL include explicit non-dependency statements naming `pipeline-foundation` (no CI infrastructure changes), `tkr-cli` (no new tkr command), and `temporal-compatibility` (no consumption of any compatibility-digest, feature-matrix, or compat-pin surface — handshake preservation is scoped to the local `worker_heartbeats` capability constant).
2. WHERE `temporal-compatibility` later lands and introduces a structural feature matrix, ANY consumption of that matrix by worker heartbeats SHALL be owned by that spec or a follow-up — not retroactively carried as a dependency of this spec.

---

## Feature 9: Correctness Properties

### Requirement 9.1: Kernel purity

**User Story:** As a Tokeira developer, I want a structural assertion that the `tokeira-kernel` crate gains no dependencies and no new transition variants from this spec, so that Kernel_Purity_Rule is preserved.

#### Acceptance Criteria

1. WHEN this spec lands, THE `crates/tokeira-kernel/Cargo.toml` dependency section SHALL NOT gain any new entry, matching the v1.62-sync Requirement 5.2 pattern.
2. WHEN this spec lands, THE `crates/tokeira-kernel/src/transitions/` module SHALL NOT gain any new transition variant.
3. THE test suite SHALL include a build-time or test-time check that parses `crates/tokeira-kernel/Cargo.toml` and asserts the dependency list matches the pre-spec state plus or minus only changes introduced by other specs.
4. IF any part of this spec's implementation would otherwise require a kernel change, THEN the implementation SHALL escalate the change to a separate kernel-facing spec and MUST NOT land the kernel change here.

### Requirement 9.2: `last_seen` monotonicity under out-of-order delivery

**User Story:** As a Tokeira developer, I want a property test asserting that the stored `last_seen` timestamp for a `(NamespaceId, WorkerInstanceKey)` entry monotonically increases across subsequent heartbeats for that key, so that out-of-order delivery or clock skew cannot silently regress eviction accounting and cause premature eviction.

#### Acceptance Criteria

1. GIVEN a sequence of `HeartbeatStore::record` calls for the same `(namespace, worker_instance_key)` with server-side receipt times `t_1, t_2, ..., t_n`, THE stored `last_seen` observed after each call SHALL be `max(t_1, ..., t_i)`. The `last_seen` SHALL NOT regress even if a later call runs with a server clock that has drifted backwards.
2. WHERE the server clock is monotonic (the normal case), the stored `last_seen` SHALL simply be the most recent call's timestamp. The `max` rule is defensive against clock skew; it matches upstream Temporal's use of `time.Now()` inside `upsertHeartbeats` under well-behaved clocks while tightening the contract for misconfigured systems.
3. THE property test SHALL live in `crates/tokeira-runtime/src/heartbeat/` and SHALL use `proptest` strategies consistent with peer-store property tests. Implements the invariant declared in Requirement 2.3.2.

### Requirement 9.3: Metric cardinality tracks registry cardinality

**User Story:** As an operator, I want a property test asserting that evicted entries do not leave orphaned metric series, so that the 1,000,000-entry store cap is also the metric-series ceiling.

#### Acceptance Criteria

1. GIVEN heartbeats for `N` distinct `worker_instance_key` values recorded across one or more namespaces, after manually invoking `HeartbeatStore::evict` to drain entries, THE observable metric label cardinality (across `heartbeats_accepted_total` and `seconds_since_last_heartbeat`) SHALL equal the post-eviction registry cardinality. Implements the invariant declared in Requirement 4.2.2.
2. THE property test SHALL additionally assert that `workers_observed{namespace}` and `workers_total` gauges match the corresponding registry counts after every eviction pass.
3. `N` SHALL be proptest-generated up to a test-bounded limit (e.g. 2,000) to keep test runtime reasonable.

### Requirement 9.4: Handler behaviour parity regression test

**User Story:** As a Tokeira developer, I want regression tests asserting that `RecordWorkerHeartbeat` and `ShutdownWorker` preserve their SDK-observable surface across this spec, so that future edits cannot silently break SDK compatibility.

#### Acceptance Criteria

1. THE test suite SHALL include unit tests that invoke `record_worker_heartbeat` with a realistic SDK-shaped payload and a deliberately-malformed payload (empty namespace, empty heartbeat vec, oversized heartbeat, missing sub-messages, out-of-range timestamps) and assert the response is `Ok(RecordWorkerHeartbeatResponse {})` or a non-`Unimplemented` status code per Requirement 3.1.
2. THE test suite SHALL include unit tests that invoke `shutdown_worker` with and without `worker_heartbeat` populated, and assert the response is `Ok(ShutdownWorkerResponse {})` or a non-`Unimplemented` status code per Requirement 3.2, with the `HeartbeatStore` correctly updated in the `Some`-heartbeat case.
3. THE tests SHALL run under the default `cargo test --workspace` gate (no Docker, no AWS credentials, no network access) consistent with `tokeira/AGENTS.md` Rule 10.
