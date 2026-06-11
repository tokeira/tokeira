# Implementation Plan: Worker Heartbeat Observability

## Overview

Promote `RecordWorkerHeartbeat` from an accept-and-discard no-op into a real observability
path without changing SDK-visible RPC behaviour. The shared heartbeat model and store trait
live in `tokeira-types` (no crate cycles); `tokeira-edge` owns proto decoding and handler
orchestration; `tokeira-runtime` owns the in-memory store, the maintenance loop, and metrics.
No kernel transition and no DSQL persistence are introduced.

> The boxes below are left **unchecked** intentionally. The implementation is believed
> complete; check each task off only after the workspace builds and the targeted suites run
> green (the verification commands are in tasks 9.x). Each task cites the requirement(s) it
> satisfies. Property tests carry their design §10 property number.

## Tasks

- [ ] 1. Shared heartbeat types and store trait in `tokeira-types`
  - [ ] 1.1 Define the compact model in `crates/tokeira-types/src/worker_heartbeat.rs`
    - `WorkerInstanceKey(pub String)` deriving `Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize`
    - `WorkerHeartbeatStatus(pub i32)` preserving the upstream status value (incl. shutdown/final)
    - `WorkerHeartbeat` with `namespace_id, worker_instance_key, task_queue, worker_identity,
      last_seen: OffsetDateTime, status, build_id: Option<String>, deployment_name: Option<String>,
      sdk_name: Option<String>, sdk_version: Option<String>`, deriving
      `Debug, Clone, PartialEq, Eq, Serialize, Deserialize`
    - Use existing `tokeira-types` newtypes and `time::OffsetDateTime`; no proto types, no
      nonexistent `Timestamp`/`Duration` aliases, no `WorkerDeploymentVersion` DTO
    - _Requirements: 1.1.1, 1.1.2, 1.1.3, 1.1.4, 1.1.5, 1.1.6, 1.1.7, 1.1.8_
  - [ ] 1.2 Define `HeartbeatStore`, `HeartbeatStoreError`, and `EvictionReport` in the same module
    - `HeartbeatStore: Send + Sync + 'static` with `insert`, `get_worker`, `list_workers`,
      and `maintain(now, ttl, min_evict_age, max_entries)`
    - `insert` upserts by `(namespace_id, worker_instance_key)`, newest wins; reads do not
      TTL-filter; `maintain` uses the caller-supplied `now`
    - `HeartbeatStoreError` is a `thiserror` enum with at least `Backend(String)`
    - `EvictionReport` carries `ttl_evicted, capacity_evicted, live, namespace_counts, remaining`
    - _Requirements: 1.2.1, 1.2.2, 1.2.3, 1.2.4, 1.2.5, 1.2.6, 1.2.7, 1.2.8_
  - [ ] 1.3 Serialization round-trip unit tests for `WorkerHeartbeat` and `WorkerInstanceKey`
    - _Requirements: 1.1.2, 1.1.3 (Testing Strategy §11)_

- [ ] 2. Decode-only edge translator
  - [ ] 2.1 Implement `worker_heartbeat_from_proto` in `crates/tokeira-edge/src/translate/worker_heartbeat`
    - Signature `(namespace_id, proto: temporal::api::worker::v1::WorkerHeartbeat, now) -> WorkerHeartbeat`
    - `last_seen = now` (server receipt time, not worker-authored `heartbeat_time`)
    - Copy `worker_instance_key, task_queue, worker_identity` verbatim (preserve empty strings)
    - Normalize empty `sdk_name`/`sdk_version` to `None`
    - Preserve upstream `status`; extract `build_id`/`deployment_name` from the deployment/version
      payload when present, else `None`
    - No validation beyond proto decode; emit per-heartbeat detail only at `trace!`
    - _Requirements: 2.1.1, 2.1.2, 2.1.3, 2.1.4, 2.1.5, 2.1.6, 2.1.7, 2.1.8, 2.1.9_
  - [ ] 2.2 Decode property test (Property 1: decode compact mirror)
    - `proptest` over upstream `WorkerHeartbeat` proto values asserting every in-scope field maps
      through, absent optionals → `None`, empty `sdk_*` → `None`, and out-of-scope sub-messages
      never cause rejection
    - _Requirements: 2.2.1, 2.2.2, 2.2.3, 2.2.4_

- [ ] 3. Runtime in-memory store and maintenance
  - [ ] 3.1 Implement `InMemoryHeartbeatStore` in `crates/tokeira-runtime/src/heartbeat.rs`
    - `new()` takes no parameters; implements `tokeira_types::HeartbeatStore`
    - Partition writes by the full `(NamespaceId, WorkerInstanceKey)` key (`DashMap` preferred;
      if manual buckets, hash the full key — never namespace alone)
    - `list_workers(namespace)` scans all shards and filters by namespace
    - On insert, `last_seen = max(existing.last_seen, incoming.last_seen)` (monotonic)
    - _Requirements: 3.1.1, 3.1.2, 3.1.3, 3.1.4, 3.1.5, 3.2.3_
  - [ ] 3.2 Implement retention constants and the `maintain` eviction logic
    - `DEFAULT_ENTRY_TTL = 24h`, `DEFAULT_MIN_EVICT_AGE = 10m`, `DEFAULT_MAX_ENTRIES = 1_000_000`,
      `DEFAULT_BUCKETS = 10`, `DEFAULT_MAINTENANCE_INTERVAL = 10s`; none added to `TokeiraConfig`
    - `maintain` evicts TTL-expired entries, applies capacity eviction respecting
      `DEFAULT_MIN_EVICT_AGE`, and returns `live` + `namespace_counts` + evicted keys
    - _Requirements: 3.2.1, 3.2.2, 3.2.5, 3.2.6_
  - [ ] 3.3 Spawn the maintenance task and thread the store into server construction
    - `spawn_heartbeat_maintenance` runs every `DEFAULT_MAINTENANCE_INTERVAL` with a single
      captured `now`, records staleness for live entries, sets `active_state = 0` for evicted
      keys, and updates count gauges
    - Runtime registers `InMemoryHeartbeatStore` as the default backing; `apps/tokeirad` threads
      `Arc<dyn HeartbeatStore>` into `WorkflowService`/`WorkflowServiceGrpc`; edge depends only on
      the `tokeira-types` trait
    - _Requirements: 3.1.6, 3.1.7, 3.2.4_
  - [ ] 3.4 Store property tests
    - Property 2 (last-write-wins), Property 3 (monotonic `last_seen`), Property 4 (shard
      distribution covers >1 bucket / DashMap sharding), Property 5 (maintenance staleness grows
      for a stopped worker), Property 6 (eviction sets `active_state = 0`; no unregister/reset)
    - _Requirements: 8.2.1, 8.2.2, 3.1.3, 3.1.4, 3.2.6, 5.1.5, 5.2.2_

- [ ] 4. Operator metrics in the runtime metrics module
  - [ ] 4.1 Register and expose the six heartbeat metrics
    - `tokeira_worker_heartbeats_accepted_total{namespace, worker_instance_key}` counter
    - `tokeira_worker_heartbeats_rejected_total{namespace, reason}` counter, reasons
      `invalid_namespace` and `store_error`
    - `tokeira_worker_heartbeat_entries_observed{namespace}` gauge
    - `tokeira_worker_heartbeat_entries_total` gauge
    - `tokeira_worker_heartbeat_active_state{namespace, worker_instance_key}` gauge (1 live / 0 evicted)
    - `tokeira_worker_last_heartbeat_age_seconds{namespace}` histogram with buckets spanning the
      SDK cadence floor to the TTL ceiling
    - Labels limited to `namespace`, `worker_instance_key`, and the finite `reason`
    - _Requirements: 5.1.1, 5.1.3, 5.1.4_
  - [ ] 4.2 Wire metric emission sites
    - Successful insert increments `..._accepted_total` and sets `active_state = 1` (incl.
      reinsertion after eviction); staleness histogram is recorded by the maintenance pass, never
      as `0.0` on accept
    - Do not unregister per-series, drop handles as removal, or reset cumulative counters
    - _Requirements: 5.1.2, 5.1.5, 5.2.1, 5.2.2, 5.2.3, 5.2.4_

- [ ] 5. Handler migration in `crates/tokeira-edge/src/grpc/workflow_service.rs`
  - [ ] 5.1 Migrate `record_worker_heartbeat`
    - Preserve v1.62-sync validation: empty `namespace` → `invalid_argument("namespace is required")`
    - Resolve namespace, decode each `worker_heartbeat`, `insert` each; empty batches succeed
    - Store failure → `Status::internal(...)` (never `Unimplemented`); success returns the
      byte-equivalent empty `RecordWorkerHeartbeatResponse {}`
    - Emit exactly one `debug!` per RPC naming namespace + heartbeat count
    - _Requirements: 4.1.1, 4.1.2, 4.1.3, 4.1.4, 4.1.5, 4.1.6_
  - [ ] 5.2 Extend `shutdown_worker`
    - Preserve existing validation; if `worker_heartbeat` present, decode and `insert` before
      `broker().deny_worker()`; on insert failure `warn!` and continue (best effort); if absent,
      no store interaction; success returns the byte-equivalent empty `ShutdownWorkerResponse {}`
    - _Requirements: 4.2.1, 4.2.2, 4.2.3, 4.2.4, 4.2.5_
  - [ ] 5.3 Handler tests (Property 7: handler parity)
    - `record_worker_heartbeat`: realistic SDK payloads, empty batch, missing sub-messages, empty
      strings, store error; `shutdown_worker`: with/without heartbeat incl. best-effort store-error
      path; assert no introduced path returns `Unimplemented` and success bodies stay empty;
      `DescribeNamespace`/`GetSystemInfo` still advertise `worker_heartbeats: true`; response proto
      types/field numbers unchanged
    - _Requirements: 4.1.x, 4.2.x, 4.3.1, 4.3.2, 4.3.3, 8.3.1, 8.3.2, 8.3.3_

- [ ] 6. Worker query backing contract
  - [ ] 6.1 Confirm the future `ListWorkers`/`DescribeWorker` data source is satisfied by the trait
    - `list_workers(namespace) -> Vec<WorkerHeartbeat>` and
      `get_worker(namespace, key) -> Option<WorkerHeartbeat>` read through `HeartbeatStore`
    - No `tokeira-projection -> tokeira-runtime` dependency and no separate materialized projection
      state is introduced
    - _Requirements: 6.1.1, 6.1.2, 6.1.3, 6.1.4, 6.1.5_

- [ ] 7. Surface_Audit amendment and guard
  - [ ] 7.1 Amend the `WorkflowService.RecordWorkerHeartbeat` row in
    `.kiro/specs/temporal-api-v1.62-sync/design.md`
    - Change classification from `No-op` to an observation-backed (`Wire through`) row owned by
      `worker-heartbeat-observability`; notes: "accept, decode compact heartbeat model, insert
      into `HeartbeatStore`, emit metrics"; full-fidelity sub-message encode remains deferred to
      `worker-deployments`
    - _Requirements: 7.1.1, 7.1.2, 7.1.3_
  - [ ] 7.2 Structural test that fails if the amended row is reverted
    - In `crates/tokeira-edge/tests/surface_audit_structure.rs`, assert the row classification is
      `Wire through` and the notes mention `HeartbeatStore`
    - _Requirements: 7.1.4_

- [ ] 8. Kernel-purity guard
  - [ ] 8.1 Test that the kernel gains no dependency and no transition variant from this spec
    - Assert `tokeira-kernel`'s `Cargo.toml` has no `tokio`/`async-trait`/`tonic` and the transition
      module list is unchanged (Property 8)
    - _Requirements: 8.1.1, 8.1.2_

- [ ] 9. Checkpoint — build, lint, and targeted suites green
  - [ ] 9.1 `cargo +nightly fmt --all --check` and `cargo lint` clean for the touched crates
  - [ ] 9.2 `cargo test -p tokeira-types` (serialization round-trips)
  - [ ] 9.3 `cargo test -p tokeira-runtime --lib heartbeat` (store + maintenance properties)
  - [ ] 9.4 `cargo test -p tokeira-edge` (translator property, handler parity, surface-audit guard)
    - Note: requires the workspace to compile; the visibility/`tokeira-storage` work in flight must
      be settled first (see `temporal-functional-conformance/reference/DIRECTION-c3-visibility.md`).

## Notes

- Boxes are intentionally unchecked. The implementation is believed complete; tick each task
  off only after the workspace compiles and the suites in task 9 run green.
- This spec is observation-only: no kernel transition, no DSQL persistence, no SDK-visible RPC
  behaviour change. Heartbeats are observations, lost on process restart by design.
- All work lives in `tokeira-types` (model + trait), `tokeira-edge` (decode + handlers), and
  `tokeira-runtime` (store, maintenance, metrics). The kernel-purity guard (task 8.1) enforces
  that the kernel is untouched.
- Task 9.4 depends on the broader workspace compiling; settle the in-flight
  `tokeira-storage`/visibility work first (see
  `temporal-functional-conformance/reference/DIRECTION-c3-visibility.md`).
- No tests require Docker, AWS, live DSQL, or network access.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2", "1.3"] },
    { "id": 1, "tasks": ["2.1", "2.2", "3.1", "3.2", "3.3", "3.4", "4.1", "4.2"] },
    { "id": 2, "tasks": ["5.1", "5.2", "5.3", "6.1", "7.1", "7.2", "8.1"] },
    { "id": 3, "tasks": ["9.1", "9.2", "9.3", "9.4"] }
  ]
}
```
