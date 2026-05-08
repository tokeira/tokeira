# Implementation Plan: Temporal API v1.43 → v1.62.11 Sync

## Overview

Resync `proto/upstream/` from `buf.build/temporalio/api:v1.43.0` to `v1.62.11`, dissolve the four interim shims introduced by `Commit_214895e`, and absorb the wire-compat delta so a `temporalio-sdk v0.4.0` worker connects, heartbeats, and round-trips an `EchoWorkflow` against the post-sync `tokeirad` without any hand-authored proto backports.

The plan follows the six-step rollout in design.md §11 Migration and Rollout:

1. Proto resync (single atomic commit).
2. Translator updates and three absorbed implementations (`CountSchedules`, `UpdateTaskQueueConfig`, Nexus v2 field wire-through) plus the `*ById` renames and the `record_worker_heartbeat` handler migration.
3. Deferred-stub blocks (31 `Unimplemented` handlers clustered into five bracketed blocks).
4. `TokeiradHandle` facade in `apps/tokeirad/src/lib.rs` (prerequisite for step 5).
5. v0.4 SDK integration test under `apps/tokeira-bench/tests/`.
6. Documentation updates.

Before the rollout, section 1 creates the placeholder spec directories that rows in the Surface_Audit point at. After the rollout, property tests validate the structural invariants on the Surface_Audit and the Impact Matrix.

Target crates and files:

- `tokeira/proto/UPSTREAM_VERSION`, `tokeira/proto/upstream/` — resync output.
- `crates/tokeira-edge/src/translate/mod.rs` — Edge_DTO additions (`SystemCapabilities`, `NamespaceCapabilities`, `NamespaceDescription`, the renamed activity DTOs, Nexus DTOs, `CountSchedulesRequest`/`Response`, `UpdateTaskQueueConfigRequest`/`Response`, `TaskQueueConfig`).
- `crates/tokeira-edge/src/grpc/translate.rs` — `system_info_to_proto`, `namespace_to_proto`, Nexus translators.
- `crates/tokeira-edge/src/grpc/workflow_service.rs` — live handlers, renamed handlers, deferred-stub blocks, `record_worker_heartbeat`.
- `crates/tokeira-edge/src/workflow_service.rs` — `SystemInfo` construction, `WorkflowServiceImpl` struct.
- `crates/tokeira-runtime/src/schedule_store.rs` — `count_schedules` method.
- `crates/tokeira-runtime/src/task_queue_config.rs` — new file with `TaskQueueConfigStore` trait and `InMemoryTaskQueueConfigStore`.
- `crates/tokeira-runtime/src/lib.rs` — re-exports.
- `crates/tokeira-projection/src/filter.rs` — `compile_schedule_filter` wrapper.
- `apps/tokeirad/src/main.rs` — store construction.
- `apps/tokeirad/src/lib.rs` — new `TokeiradHandle` facade.
- `apps/tokeira-bench/tests/v0_4_integration.rs` — new `#[ignore]` integration test.
- `.kiro/specs/<placeholder>/.placeholder.md` — 8 new placeholder spec directories.

## Tasks

- [ ] 1. Create placeholder spec directories for every deferred Surface_Audit target spec that does not yet exist
  - Each placeholder is a `.kiro/specs/<name>/.placeholder.md` file containing a one-paragraph scope statement and a pointer back to `temporal-api-v1.62-sync` as the spec that identified the need. Property 3 in design.md requires every Surface_Audit Target Spec column value to exist as a directory under `.kiro/specs/`; this section satisfies that invariant for the 8 names that are not already present.
  - [ ] 1.1 Create `tokeira/.kiro/specs/worker-deployments/.placeholder.md`
    - Scope: the 11 Worker Deployments RPCs enumerated in design.md §5 Surface_Audit and the `temporal.api.deployment.v1` messages consumed by them.
    - Flip target for `SystemCapabilities.server_scaled_deployments` from `false` to `true` when this spec lands.
    - (Req 2.1.3 Worker Deployments rows, Req 6.2, Property 3)
  - [ ] 1.2 Create `tokeira/.kiro/specs/workflow-rules/.placeholder.md`
    - Scope: the 5 Workflow Rules RPCs and the `temporal.api.rules.v1` package.
    - (Req 2.1.3 Workflow Rules rows, Req 6.3.1, Property 3)
  - [ ] 1.3 Create `tokeira/.kiro/specs/activity-executions-first-class/.placeholder.md`
    - Scope: the 8 Activity Executions RPCs and the new kernel representation of pending activities as durable addressable objects.
    - (Req 2.1.3 Activity Executions rows, Req 6.3.2, Property 3)
  - [ ] 1.4 Create `tokeira/.kiro/specs/worker-config-management/.placeholder.md`
    - Scope: `FetchWorkerConfig` and `UpdateWorkerConfig` with a server-side config store for SDK workers.
    - (Req 2.1.3 Worker Config row, Property 3)
  - [ ] 1.5 Create `tokeira/.kiro/specs/kernel-pause-workflow/.placeholder.md`
    - Scope: first-class `PauseWorkflowExecution` / `UnpauseWorkflowExecution` as kernel transitions, distinct from the v1.43 activity-level pause-by-id surface.
    - (Req 2.1.3 Pause/Unpause Workflow row, Property 3)
  - [ ] 1.6 Create `tokeira/.kiro/specs/worker-heartbeat-observability/.placeholder.md`
    - Scope: persistent `WorkerHeartbeat` storage, kernel-observed worker liveness, `ListWorkers` projection, and the promotion of `record_worker_heartbeat` from no-op to real handler.
    - (Req 2.1.3 RecordWorkerHeartbeat row, Req 3.3.3, Req 3.4.4, Property 3)
  - [ ] 1.7 Create `tokeira/.kiro/specs/nexus-retry-policy/.placeholder.md`
    - Scope: runtime retry branching on `NexusRetryBehavior` when Nexus-specific retry shapes are needed. Referenced by the Impact Matrix escalation for `RespondNexusTaskFailedRequest.error.retry_behavior`.
    - (Req 5.1.3 escalation note for `RespondNexusTaskFailedRequest.error.retry_behavior`, Property 3, Property 7)
  - [ ] 1.8 Create `tokeira/.kiro/specs/speculative-wft/.placeholder.md`
    - Scope: speculative workflow tasks as a distinct task kind. Consumer of `RespondWorkflowTaskCompletedRequest.Capabilities.discard_speculative_workflow_task_with_events`.
    - (Req 4.2.3 follow-up spec reference, Property 3)

- [ ] 2. Proto resync to v1.62.11 (§11 Migration Step 1, single atomic commit)
  - [ ] 2.1 Invoke `Proto_Sync_Tool` against v1.62.11
    - From `tokeira/` workspace root, run `cargo run -p proto-sync -- v1.62.11`.
    - The tool wipes `proto/upstream/temporal/api/`, runs `buf export buf.build/temporalio/api:v1.62.11 --output proto/upstream/`, and writes `proto/UPSTREAM_VERSION`.
    - Do NOT modify any file under `tokeira/tools/proto-sync/` — the tool is owned by the `proto-upstream-sync` spec and consumed unchanged.
    - (Req 1.1.1, 1.1.5, §1 Proto sync invocation)
  - [ ] 2.2 Verify `UPSTREAM_VERSION` pin and new packages
    - Assert `proto/UPSTREAM_VERSION` contains exactly `v1.62.11\n`.
    - Assert the following files now exist under `proto/upstream/`: `temporal/api/worker/v1/message.proto`, `temporal/api/rules/v1/message.proto`, `temporal/api/protometa/v1/annotations.proto`.
    - Assert the v1.62.11 generated `workflowservice::workflow_service_server::WorkflowService` trait contains every RPC enumerated in the Surface_Audit §5 under "New RPCs on WorkflowService".
    - (Req 1.1.2, 1.1.3, 1.2.1, 1.2.2, 1.2.3, 1.2.4, 1.2.5)
  - [ ] 2.3 Verify `Commit_214895e` backports are absent
    - `rg "Tokeirad currently accepts heartbeats as a no-op" proto/upstream/` SHALL return zero matches.
    - `rg "A production implementation is tracked in a follow-up spec" proto/upstream/` SHALL return zero matches.
    - The hand-authored `worker_heartbeats = 4;` field on `NamespaceInfo.Capabilities` in `proto/upstream/temporal/api/namespace/v1/message.proto` and the hand-authored `rpc RecordWorkerHeartbeat` + `RecordWorkerHeartbeatRequest.worker_heartbeat = repeated bytes` in `service.proto` / `request_response.proto` SHALL be absent — the post-sync tree contains only the upstream vendor output.
    - Assert the generated `RecordWorkerHeartbeatRequest` type exposes `worker_heartbeat: Vec<temporal::api::worker::v1::WorkerHeartbeat>` rather than `Vec<Vec<u8>>`.
    - (Req 3.1.1, 3.1.3, 3.2.1, 3.2.2, 3.2.3, 3.5.1)
  - [ ] 2.4 Resolve signature drift to restore a green workspace
    - Run `cargo build --workspace` and fix the minimum set of translator / handler compile errors caused by renamed, reordered, or retyped generated fields. The fix is scoped to restoring compilation; substantive behavioural changes are deferred to sections 4–10 below.
    - Run `cargo clippy --workspace --all-targets` and resolve any new warnings surfaced by the resync.
    - Run `cargo test --workspace` (excluding `#[ignore]`'d tests) and fix any test compile drift.
    - (Req 1.3.1, 1.3.2, 1.3.3)

- [ ] 3. Checkpoint — proto resync landed cleanly
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 4. Edge DTO extensions (§11 Migration Step 2, part 1; §2 Edge DTO additions)
  - Every DTO listed below lives in `crates/tokeira-edge/src/translate/mod.rs` unless otherwise noted. Each addition is a pure data-structure change; behaviour lands in sections 5–10.
  - [ ] 4.1 Extend `SystemCapabilities` with v1.62 fields
    - Add `server_scaled_deployments: bool` and `worker_heartbeats: bool` to `SystemCapabilities` per §2 Data Models.
    - Field rationale comments per §2: `server_scaled_deployments` defaults to `false` (Worker Deployments deferred); `worker_heartbeats` defaults to `true` (no-op handler accepts calls).
    - (Req 4.1.1, 4.1.2)
  - [ ] 4.2 Add `NamespaceCapabilities` and extend `NamespaceDescription`
    - Add a new `NamespaceCapabilities` struct with `worker_heartbeats: bool` and `reported_problems_search_attribute: bool`.
    - Extend `NamespaceDescription` to carry a `capabilities: NamespaceCapabilities` field.
    - Mirror every v1.62 `NamespaceInfo` / `NamespaceConfig` addition classified `Classification_WireThrough` in the Surface_Audit onto the corresponding nested DTOs; do NOT mirror additions classified `Classification_Deferred` (e.g. `NamespaceInfo.supported_clients`).
    - For Classification_Deferred namespace-config additions, the translator SHALL emit the protobuf default on the response path (not re-synthesise a non-default value) per Req 4.4.2.
    - (Req 2.2.6, 4.1.4, 4.4.1, 4.4.2, 4.4.3)
  - [ ] 4.3 Extend `RespondWorkflowTaskCompletedRequest` with the speculative-task capability
    - Add `client_discards_speculative_with_events: bool` to `RespondWorkflowTaskCompletedRequest` per §2 Data Models.
    - Comment on the DTO field: "decoded at the edge; not propagated downstream; consumed by a future `speculative-wft` spec."
    - (Req 4.2.1, 4.2.3, 4.2.4)
  - [ ] 4.4 Add `CountSchedulesRequest` and `CountSchedulesResponse` DTOs
    - Fields per §2 Data Models: `CountSchedulesRequest { namespace: String, query: Option<String> }`, `CountSchedulesResponse { count: u64 }`.
    - Follow the naming and structure of the existing `CountWorkflowExecutionsRequest` / `Response` DTOs re-exported from `tokeira-projection`.
    - (Req 4.6.5)
  - [ ] 4.5 Add `UpdateTaskQueueConfigRequest`, `UpdateTaskQueueConfigResponse`, and `TaskQueueConfig` DTOs
    - Fields per §2 Data Models: `TaskQueueConfig { rate_limit_override: Option<f64>, description: String, tier_hint: Option<String> }`; `UpdateTaskQueueConfigRequest { namespace, task_queue, config }`; `UpdateTaskQueueConfigResponse { applied: TaskQueueConfig }`.
    - (Req 4.7.1, 4.7.2)
  - [ ] 4.6 Rename the `*ById` activity DTOs to drop the suffix
    - Rename `UpdateActivityOptionsByIdRequest` → `UpdateActivityOptionsRequest`, `PauseActivityByIdRequest` → `PauseActivityRequest`, `UnpauseActivityByIdRequest` → `UnpauseActivityRequest`, `ResetActivityByIdRequest` → `ResetActivityRequest` in the DTO module.
    - Add the v1.62 wire-through fields identified in the Impact Matrix: `UpdateActivityOptionsRequest.activity_type: Option<ActivityType>`, `PauseActivityRequest.identity: String`, `UnpauseActivityRequest.reset_heartbeat: bool`, `ResetActivityRequest.keep_paused: bool`.
    - Handler and caller renames are owned by section 9; this sub-task covers only the DTO renames and new DTO fields.
    - (Req 4.3.4, Impact Matrix rows for renamed request DTOs)
  - [ ] 4.7 Extend the Nexus DTO family with v1.62 field additions
    - In `crates/tokeira-edge/src/translate/nexus.rs`, add the wire-through fields enumerated in the Surface_Audit Nexus section: `PollNexusTaskQueueResponse.poll_request_id: String`, expanded `PollNexusTaskQueueResponse.request` sub-fields, expanded `RespondNexusTaskCompletedRequest.response` sub-fields, `NexusEndpointSpec.description: String`, and the new `NexusEndpointSpec.endpoint_type` enum variant.
    - Fields classified `Classification_Deferred` (notably `NexusEndpointSpec.allowed_cluster_ids` and `RespondNexusTaskFailedRequest.error.retry_behavior` after Impact Matrix escalation) SHALL be explicitly dropped at the edge per tightened Req 2.2.6 — they are NOT mirrored onto DTOs, NOT carried as opaque bytes, and the response path emits the protobuf default.
    - (Req 4.8.1, 4.8.2, 4.8.4, 4.8.5)
  - [ ]* 4.8 Write property test P1: translator round-trip fidelity
    - **Property 1: Translator round-trip fidelity**
    - **Validates: Requirements 2.2.5, 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, and every in-scope row in the Impact Matrix (§6 design.md)**
    - For each DTO type touched in sub-tasks 4.1–4.7 and 4.9, generate arbitrary instances via `proptest` strategies, encode via `*_to_proto`, decode via `*_from_proto`, and assert byte-equivalence on every field the translator preserves.
    - Every `Classification_WireThrough` field listed in the Impact Matrix MUST be included in the preserved-field comparison. The `client_discards_speculative_with_events` DTO field (Req 4.2.1–4.2.4) MUST also be included because Req 4.2 requires the edge to decode and preserve it even though it is classified `Classification_Capability` rather than `Classification_WireThrough`. Only fields with `Classification_Deferred` classification are excluded, and each exclusion cites the Surface_Audit row that justifies it per Req 4.5.2.
    - Test location: `crates/tokeira-edge/src/translate/` submodule test modules; minimum 256 iterations per `proptest::test_runner::Config::default()`.
  - [ ] 4.9 Wire-through field additions on StartWorkflow / SignalWithStartWorkflow / Poll / Respond / DescribeTaskQueue / ScheduleSpec / enum families
    - In `crates/tokeira-edge/src/translate/mod.rs` and the neighbouring translator modules, add every `Classification_WireThrough` field listed in the Impact Matrix that is not already covered by sub-tasks 4.1–4.7:
      - `StartWorkflowExecutionRequest.user_metadata: Option<UserMetadata>`, `.links: Vec<Link>`, `.priority: Option<Priority>`, `.completion_callbacks: Vec<Callback>`.
      - `SignalWithStartWorkflowExecutionRequest.user_metadata`, `.links`, `.priority` (mirroring StartWorkflow).
      - `PollWorkflowTaskQueueResponse.poll_request_id: String`.
      - `PollActivityTaskQueueResponse.priority: Option<Priority>`, `.poll_request_id: String`.
      - `RecordActivityTaskHeartbeatRequest.worker_version: Option<WorkerVersionStamp>`.
      - `RespondActivityTaskCompletedRequest.worker_version`, `RespondActivityTaskFailedRequest.worker_version`, `RespondActivityTaskCanceledRequest.worker_version`.
      - `DescribeTaskQueueResponse.task_queue_stats: Option<TaskQueueStats>`.
      - Enum additions: `TaskReachability` new variants, `BuildIdTaskReachability`, `ApplicationErrorCategory`.
    - For each field, add the DTO field, update `*_from_proto` and `*_to_proto`, and update the construction / consumption call sites identified in the Impact Matrix Implementation Notes column (typically a single-file edit each).
    - `DescribeTaskQueueResponse.config` is owned by sub-task 7.6; do NOT duplicate it here.
    - Fields explicitly classified `Classification_Deferred` (e.g. `StartWorkflowExecutionRequest.versioning_override`, `RespondActivityTaskFailedRequest.is_last_failure`) are out of scope for this sub-task and are explicitly dropped per tightened Req 2.2.6.
    - (Req 2.2.5, Impact Matrix in-scope rows that sub-tasks 4.1–4.7 do not already cover)

- [ ] 5. Capability advertisement in edge translators (§11 Migration Step 2, part 2; §3 Capability advertisement)
  - [ ] 5.1 Update `system_info_to_proto` to emit v1.62 capability flags
    - In `crates/tokeira-edge/src/grpc/translate.rs` around lines 825–848, extend the returned `workflowservice::get_system_info_response::Capabilities` with `server_scaled_deployments: sys.capabilities.server_scaled_deployments` and `worker_heartbeats: sys.capabilities.worker_heartbeats`.
    - Drive values from the `SystemCapabilities` DTO, not from literals; flipping a flag is a one-line change at the construction site in sub-task 5.3.
    - Populate any additional Classification_Capability fields identified in the Surface_Audit (e.g. `nexus`, `sdk_metadata`, `count_group_by_execution_status`) from the DTO where not already wired.
    - (Req 4.1.3)
  - [ ] 5.2 Update `namespace_to_proto` to emit v1.62 namespace capability flags
    - In `crates/tokeira-edge/src/grpc/translate.rs` around line 865, populate `namespace_proto::namespace_info::Capabilities { worker_heartbeats: desc.capabilities.worker_heartbeats, reported_problems_search_attribute: desc.capabilities.reported_problems_search_attribute, .. }` from the DTO.
    - Replace the `Commit_214895e` literal with DTO-driven values.
    - Update the rationale comment to reference `temporal-api-v1.62-sync` and name `worker-heartbeat-observability` as the spec owning real observability. Do NOT rely on `..Default::default()` for new capability flags — write the values out verbatim so the classification is visible at the call site.
    - (Req 3.3.1, 3.3.2, 3.3.3, 4.1.4)
  - [ ] 5.3 Update the `SystemInfo` construction site in `workflow_service.rs`
    - In `crates/tokeira-edge/src/workflow_service.rs` around lines 2283–2288, populate the new `SystemCapabilities` fields explicitly: `server_scaled_deployments: false` (Worker Deployments deferred) and `worker_heartbeats: true` (no-op handler keeps the SDK alive). Do NOT use `..Default::default()` for these fields.
    - (Req 4.1.5, 3.5.2)

- [ ] 6. `CountSchedules` implementation (§11 Migration Step 2, part 3; §4 CountSchedules impl)
  - [ ] 6.1 Extend `ScheduleStore` with `count_schedules`
    - In `crates/tokeira-runtime/src/schedule_store.rs`, add `pub fn count_schedules(&self, namespace: &NamespaceId, query: Option<&str>) -> Result<u64, ScheduleCountError>` per §4 Data Models. Implement on the existing in-memory `ScheduleStore::default()` backing.
    - Add `pub enum ScheduleCountError { UnsupportedQuery }` with `#[derive(Debug, thiserror::Error)]`.
    - `None` query counts all schedules in the namespace; `Some(q)` delegates to `tokeira_projection::filter::compile_schedule_filter(q)` (added in sub-task 6.2) and filters the entry list.
    - (Req 4.6.2)
  - [ ] 6.2 Add `compile_schedule_filter` wrapper in `tokeira-projection`
    - In `crates/tokeira-projection/src/filter.rs`, add `pub fn compile_schedule_filter(query: &str) -> Result<ScheduleFilter, FilterError>` that restricts the permitted field set to `schedule_id`, `namespace`, `paused`, `notes`, and custom search attributes, and the permitted operators to `eq`, `in`.
    - Unsupported syntax, unsupported fields, malformed expressions — all yield a single error variant that maps at the edge to `Status::invalid_argument("unsupported schedule query")`.
    - Re-export `ScheduleFilter` from the filter module so `ScheduleStore::count_schedules` can type its parameter.
    - (Req 4.6.3)
  - [ ] 6.3 Implement the `count_schedules` gRPC handler
    - In `crates/tokeira-edge/src/grpc/workflow_service.rs`, implement `async fn count_schedules` per §4 CountSchedules impl.
    - Return `Status::invalid_argument("namespace is required")` on empty namespace, `Status::not_found("namespace not found")` on unknown namespace (NOT `Ok(0)`), and `Status::invalid_argument("unsupported schedule query")` on `ScheduleCountError::UnsupportedQuery`.
    - (Req 4.6.1, 4.6.3, 4.6.4)
  - [ ]* 6.4 Write property test P4: `count_schedules` count semantics
    - **Property 4: `count_schedules` count semantics**
    - **Validates: Requirements 4.6.1, 4.6.2, 4.6.3**
    - `count_schedules(namespace, None)` equals the number of entries in the namespace. `count_schedules(namespace, Some(q))` for any valid filter `q` is `≤` the no-filter count. Determinism: two calls with equal arguments and no mutation return equal results. Any `q` rejected by `compile_schedule_filter` yields `ScheduleCountError::UnsupportedQuery`.
    - Test location: `crates/tokeira-runtime/src/schedule_store.rs` `#[cfg(test)]` module; minimum 256 iterations.

- [ ] 7. `UpdateTaskQueueConfig` implementation (§11 Migration Step 2, part 4; §5 UpdateTaskQueueConfig impl)
  - [ ] 7.1 Add `TaskQueueConfigStore` trait and `InMemoryTaskQueueConfigStore` backing
    - Create `crates/tokeira-runtime/src/task_queue_config.rs` with `TaskQueueConfigEntry`, the `TaskQueueConfigStore` trait (`get` / `set` / `list`), and `InMemoryTaskQueueConfigStore` using `DashMap<(NamespaceId, String), TaskQueueConfigEntry>` per §5 Data Models.
    - Follow the shape and construction convention of the existing `ScheduleStore` and `VersioningRuleStore`.
    - (Req 4.7.2)
  - [ ] 7.2 Re-export the store from `tokeira_runtime`
    - In `crates/tokeira-runtime/src/lib.rs`, add `pub mod task_queue_config;` and re-export `TaskQueueConfigStore`, `TaskQueueConfigEntry`, `InMemoryTaskQueueConfigStore`.
    - (Req 4.7.2)
  - [ ] 7.3 Construct the store in `apps/tokeirad/src/main.rs`
    - Around lines 125–150, alongside `ScheduleStore::default()` and `VersioningRuleStore::default()`, add `let task_queue_config_store: Arc<dyn TaskQueueConfigStore> = Arc::new(InMemoryTaskQueueConfigStore::default());` and thread it through the `WorkflowServiceImpl` construction.
    - (Req 4.7.2)
  - [ ] 7.4 Add the `task_queue_config_store` field to `WorkflowServiceImpl`
    - In `crates/tokeira-edge/src/workflow_service.rs`, add `task_queue_config_store: Arc<dyn TaskQueueConfigStore>` to the struct and constructor.
    - (Req 4.7.2)
  - [ ] 7.5 Implement the `update_task_queue_config` gRPC handler
    - In `crates/tokeira-edge/src/grpc/workflow_service.rs`, implement `async fn update_task_queue_config` per §5 UpdateTaskQueueConfig impl.
    - Validate empty `namespace` → `Status::invalid_argument("namespace is required")`; empty `task_queue` → `Status::invalid_argument("task queue is required")`; unknown namespace → `Status::not_found("namespace not found")`.
    - The `TaskQueueConfigStore::set` call is infallible on the in-memory backing; no further error paths are introduced.
    - (Req 4.7.1, 4.7.4)
  - [ ] 7.6 Update `describe_task_queue` to read from `TaskQueueConfigStore`
    - Populate the `config` field on `DescribeTaskQueueResponse` from `self.task_queue_config_store.get(&namespace_id, &req.task_queue)`. A `None` returns the default `TaskQueueConfig` (all fields at protobuf defaults), matching upstream semantics.
    - This sub-task SHALL NOT alter task-queue admission, polling, or dispatch behaviour (Req 4.7.5 — rate-limit enforcement is deferred to a future admission-control spec).
    - (Req 4.7.3)
  - [ ]* 7.7 Write property test P5: `TaskQueueConfigStore` set/get round-trip
    - **Property 5: `TaskQueueConfigStore` set/get round-trip**
    - **Validates: Requirements 4.7.1, 4.7.2**
    - For any `(namespace, task_queue, config)` triple, `set` then `get` returns `Some(cfg)` with `cfg == config`. For any pair of distinct `(namespace_a, task_queue_a) != (namespace_b, task_queue_b)`, setting under one key does not affect the value under the other (key isolation).
    - Test location: `crates/tokeira-runtime/src/task_queue_config.rs` `#[cfg(test)]` module; minimum 256 iterations.

- [ ] 8. Nexus v2 field wire-through (§11 Migration Step 2, part 5; §6 Nexus v2 wire-through)
  - [ ] 8.1 Decode v1.62 Nexus fields in `crates/tokeira-edge/src/translate/nexus.rs`
    - Update `*_from_proto` translators for `PollNexusTaskQueueResponse`, `RespondNexusTaskCompletedRequest`, `RespondNexusTaskFailedRequest`, and the `NexusEndpointSpec` family to copy every new field from the proto into the DTO. Re-emit the same fields on the `*_to_proto` return path.
    - Skip fields escalated to `Classification_Deferred` in the Impact Matrix (e.g. `RespondNexusTaskFailedRequest.error.retry_behavior`, `NexusEndpointSpec.allowed_cluster_ids`).
    - (Req 4.8.1, 4.8.2, 4.8.5)
  - [ ] 8.2 Pass the new Nexus fields through `NexusTaskBroker`
    - In `tokeira-runtime`, extend whichever internal state types already carry the affected Nexus message so that each new DTO field is carried through without new behavioural coupling to dispatch or retry.
    - (Req 4.8.2)
  - [ ] 8.3 Handle new `NexusEndpointSpec.endpoint_type` variants in `NexusEndpointRegistry`
    - Add a match arm in `NexusEndpointRegistry::resolve` for the new endpoint-type variant(s). Unrouteable-today variants return `NexusResolution::Failed { message: format!("nexus endpoint type {:?} not yet routed", endpoint_type) }`, matching the pattern used for unknown endpoints.
    - (Req 4.8.3)

- [ ] 9. `*ById` → unsuffixed RPC renames (§11 Migration Step 2, part 6; §8 RPC renames)
  - [ ] 9.1 Rename the four activity-management handler methods
    - In `crates/tokeira-edge/src/grpc/workflow_service.rs`, rename `update_activity_options_by_id` → `update_activity_options`, `pause_activity_by_id` → `pause_activity`, `unpause_activity_by_id` → `unpause_activity`, `reset_activity_by_id` → `reset_activity`. Method bodies are preserved modulo signature drift from the renamed message types (`PauseActivityRequest`, etc.).
    - The v1.43 RPC names no longer exist in the generated trait; any orphan methods on the impl block must be removed or renamed in this sub-task to keep the trait satisfied.
    - (Req 4.3.1, 4.3.2)
  - [ ] 9.2 Wire the v1.62 field additions on the renamed request messages
    - For `UpdateActivityOptionsRequest.activity_type`, `PauseActivityRequest.identity`, `UnpauseActivityRequest.reset_heartbeat`, `ResetActivityRequest.keep_paused` — update the existing runtime-facing handlers to read the new DTO fields and pass them through. Impact Matrix classifies each as a single-file edit; no new runtime state types are introduced.
    - (Req 4.3.3, Impact Matrix rows for renamed activity request fields)
  - [ ] 9.3 Update all callers of the renamed DTOs
    - Rename references to `PauseActivityByIdRequest` / etc. across the workspace (tests, helper functions, docs) to their unsuffixed forms.
    - (Req 4.3.4)

- [ ] 10. `record_worker_heartbeat` handler migration (§11 Migration Step 2, part 7; §9 record_worker_heartbeat migration)
  - [ ] 10.1 Accept the upstream-typed request
    - In `crates/tokeira-edge/src/grpc/workflow_service.rs` around line 621, update `record_worker_heartbeat` to accept `Request<workflowservice::RecordWorkerHeartbeatRequest>` where `RecordWorkerHeartbeatRequest.worker_heartbeat` is `Vec<temporal::api::worker::v1::WorkerHeartbeat>` (upstream-generated, no longer `Vec<Vec<u8>>`).
    - Return `Ok(Response::new(workflowservice::RecordWorkerHeartbeatResponse {}))` with no side effects on Kernel, Runtime, Storage, or Projection.
    - (Req 3.4.1, 3.4.2)
  - [ ] 10.2 Validate namespace is non-empty and emit a single debug log per call
    - On empty `req.namespace`, return `Status::invalid_argument("namespace is required")` — match the `shutdown_worker` convention at `workflow_service.rs` lines 636–640.
    - Emit exactly one `tracing::debug!` line per call including `rpc = "RecordWorkerHeartbeat"`, `namespace = %req.namespace`, and `heartbeat_count = req.worker_heartbeat.len()`. Do NOT emit at `info` or higher — a v0.4 worker heartbeats every 30 s per registered worker.
    - (Req 3.4.3, 3.4.5)
  - [ ] 10.3 Update the rationale comment
    - Replace the `Commit_214895e` rationale comment with one that names `temporal-api-v1.62-sync` as the spec that established the current shape and `worker-heartbeat-observability` as the spec that owns real persistent observability.
    - (Req 3.4.4, 3.5.2)

- [ ] 11. Checkpoint — Step 2 (translator updates and absorbed implementations) complete
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 12. Deferred-stub blocks (§11 Migration Step 3; §7 Stub handler blocks)
  - Every Classification_Deferred RPC returns `Err(Status::unimplemented(format!("{} is not implemented; tracked in spec {}", rpc_name, target_spec)))` with a single `tracing::debug!` log line per call. Never `warn!` or higher. Blocks live at the end of `crates/tokeira-edge/src/grpc/workflow_service.rs`, bracketed by leading and trailing comments per Req 6.2.1.
  - [ ] 12.1 Worker Deployments stub block (11 RPCs)
    - Bracket with `// === Worker Deployments — deferred to worker-deployments spec ===` and `// === End Worker Deployments block ===`.
    - Implement stubs for `describe_worker`, `list_workers`, `describe_worker_deployment`, `describe_worker_deployment_version`, `set_worker_deployment_current_version`, `set_worker_deployment_ramping_version`, `delete_worker_deployment`, `delete_worker_deployment_version`, `list_worker_deployments`, `update_worker_deployment_version_metadata`, `set_worker_deployment_manager`.
    - (Req 6.1.1, 6.1.2, 6.1.3, 6.1.4, 6.2.1, 6.2.2)
  - [ ] 12.2 Workflow Rules stub block (5 RPCs)
    - Bracket with `// === Workflow Rules — deferred to workflow-rules spec ===` and `// === End Workflow Rules block ===`.
    - Implement stubs for `create_workflow_rule`, `describe_workflow_rule`, `delete_workflow_rule`, `list_workflow_rules`, `trigger_workflow_rule`.
    - (Req 6.3.1)
  - [ ] 12.3 Activity Executions stub block (8 RPCs)
    - Bracket with `// === Activity Executions — deferred to activity-executions-first-class spec ===` and `// === End Activity Executions block ===`.
    - Implement stubs for `start_activity_execution`, `describe_activity_execution`, `poll_activity_execution`, `list_activity_executions`, `count_activity_executions`, `request_cancel_activity_execution`, `terminate_activity_execution`, `delete_activity_execution`.
    - (Req 6.3.2)
  - [ ] 12.4 Worker Config stub block (2 RPCs)
    - Bracket with `// === Worker Config — deferred to worker-config-management spec ===` and `// === End Worker Config block ===`.
    - Implement stubs for `fetch_worker_config`, `update_worker_config`.
    - (Req 6.1, Surface_Audit Worker Config rows)
  - [ ] 12.5 Pause/Unpause Workflow stub block (2 RPCs)
    - Bracket with `// === Pause/Unpause Workflow — deferred to kernel-pause-workflow spec ===` and `// === End Pause/Unpause Workflow block ===`.
    - Implement stubs for `pause_workflow_execution`, `unpause_workflow_execution`.
    - (Req 6.1, Surface_Audit Pause/Unpause rows)
  - [ ] 12.6 Verify stub coverage preserves the v1.43-era Unimplemented set
    - Audit the RPCs that returned `Status::unimplemented(...)` before this spec and assert every one still returns `Status::unimplemented(...)` unless this spec explicitly classifies it into `Classification_NoOp`, `Classification_Capability`, or `Classification_WireThrough`.
    - No new RPC outside the Surface_Audit gains a non-`Unimplemented` handler in this spec.
    - (Req 6.4.1, 6.4.2)
  - [ ]* 12.7 Write property test P6: deferred-handler response format
    - **Property 6: Deferred-handler response format**
    - **Validates: Requirements 6.1.1, 6.1.2, 6.1.3, 6.1.4**
    - Enumerate every RPC in the Surface_Audit classified `Classification_Deferred`. For each, call the handler on `WorkflowServiceImpl` and assert the result is `Err(Status::unimplemented(msg))` where `msg` contains the exact RPC name, the exact deferring spec name, and the word "implemented" or "tracked".
    - Assert exactly one `tracing::debug!` line per call via a test-only tracing subscriber; assert zero `warn!` or higher log lines.
    - Test location: `crates/tokeira-edge/tests/grpc_deferred_handlers.rs`.

- [ ] 13. Surface_Audit and Impact Matrix structural property tests
  - [ ]* 13.1 Write property test P2: Surface_Audit wire-through count matches in-scope Impact Matrix row count
    - **Property 2: Surface_Audit wire-through count matches in-scope Impact Matrix row count**
    - **Validates: Requirements 2.3, 2.3.3, 5.1.1**
    - Parse the Surface_Audit table in `design.md`. Assert the count of rows with `Classification == "Wire through"` equals the count of rows in the Impact Matrix table whose Implementation Notes column starts with `In scope` (i.e. was not escalated to `Classification_Deferred` per Req 5.1.3). Impact Matrix rows whose Implementation Notes column starts with `**Classified Deferred**` are excluded from the equivalence because they already appear as `Classification_Deferred` rows in the Surface_Audit — counting them on both sides would double-count. Rows classified `No-op/compile-only` (e.g. `temporal.api.worker.v1` and its sub-messages) are also excluded from the wire-through count because no translator edit corresponds to them.
    - Complement property: the count of Surface_Audit rows with `Classification == "Deferred"` SHALL be ≥ the count of Impact Matrix `**Classified Deferred**` rows. (The Surface_Audit additionally carries Classification_Deferred RPCs and messages that never reach the Impact Matrix because they have no in-scope Implementation Notes to record.)
    - Test location: `crates/tokeira-edge/tests/surface_audit_structure.rs`.
  - [ ]* 13.2 Write property test P3: every deferred spec name exists as a workspace directory
    - **Property 3: every deferred spec name exists as a workspace directory**
    - **Validates: Requirements 2.1, 2.1.3, 8.1.2, Property 3**
    - Parse the Surface_Audit table. For every row classified `Classification_Deferred`, assert the Target Spec column value exists as a directory under `.kiro/specs/` in the workspace. The set covered includes `worker-deployments`, `worker-heartbeat-observability`, `workflow-rules`, `activity-executions-first-class`, `worker-config-management`, `kernel-pause-workflow`, `runtime-worker-versioning`, `runtime-activity-timeouts`, `nexus-retry-policy`, `speculative-wft`, and `temporal-compatibility`.
    - Test location: `crates/tokeira-edge/tests/surface_audit_structure.rs`.
  - [ ]* 13.3 Write property test P7: Impact Matrix escalation invariant
    - **Property 7: Impact Matrix escalation invariant**
    - **Validates: Requirements 5.1.3, 5.1.4, 5.1.5, 5.2**
    - For every row in the Impact Matrix: non-`none` Kernel Impact implies escalation to `Classification_Deferred` or the column value is exactly `existing transition field`; non-`none` Runtime Impact exceeding a single-file edit implies escalation; non-`none` Projection Impact requiring a migration file implies escalation.
    - Additionally assert `crates/tokeira-kernel/Cargo.toml` gained no new dependency entries from this spec, and `crates/tokeira-kernel/` gained no new `use` statements on `tokio`, `async_trait`, `tonic`, or `prost` — parse the crate's `src/` tree for these imports.
    - Test location: `crates/tokeira-edge/tests/surface_audit_structure.rs`.

- [ ] 14. Checkpoint — Step 3 (deferred-stub blocks + structural tests) complete
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 15. `TokeiradHandle` facade (§11 Migration Step 4; §3 component 10 prerequisite)
  - [ ] 15.1 Expose `apps/tokeirad/src/lib.rs` with `TokeiradHandle::start_in_memory`
    - Create `apps/tokeirad/src/lib.rs` exporting `TokeiradHandle` and `pub async fn start_in_memory(addr: SocketAddr) -> anyhow::Result<TokeiradHandle>`.
    - `start_in_memory` wires the same in-memory storage path `tokeirad main()` uses when started with `--storage in-memory`, binds to the caller-provided ephemeral socket, and returns a handle whose `Drop` tears down the runtime cleanly.
    - Expose `TokeiradHandle::bound_addr()`, `TokeiradHandle::log_sink()` (returning a `tokio::sync::broadcast::Receiver` over tracing events), and `TokeiradHandle::shutdown(self) -> anyhow::Result<()>`.
    - Update `apps/tokeirad/Cargo.toml` to declare both `[lib]` and `[[bin]]` targets.
    - (Req 7.1.2, §3 component 10 prerequisite)
  - [ ] 15.2 Refactor `apps/tokeirad/src/main.rs` to a thin wrapper over the facade
    - `fn main()` parses CLI args and delegates to the facade. All wiring logic moves to `lib.rs`.
    - No behavioural change to the binary CLI surface; existing unit tests against `main.rs` continue to pass or are moved to `lib.rs`.
    - (§3 component 10 prerequisite)
  - [ ] 15.3 Add a facade unit test for clean startup and shutdown
    - In `apps/tokeirad/tests/facade.rs`, bind `TokeiradHandle::start_in_memory("127.0.0.1:0".parse()?)`, assert `bound_addr().port() != 0`, call `shutdown()`, assert clean exit. Use `tokio::sync::Notify` for any synchronisation; no `tokio::time::sleep`.
    - Run via `cargo test --package tokeirad`.
    - (§11 Migration Step 4 acceptance gate)

- [ ] 16. v0.4 SDK integration test (§11 Migration Step 5; §3 component 10)
  - [ ] 16.1 Write `apps/tokeira-bench/tests/v0_4_integration.rs`
    - Implement the single `#[tokio::test]` function per §3 component 10 and §10 Testing Strategy.
    - Steps: spawn `tokeirad::TokeiradHandle::start_in_memory` on `127.0.0.1:0`; subscribe to `log_sink()` and watch for `RecordWorkerHeartbeat` debug lines; instantiate `temporalio_client::Client` v0.4 against `bound_addr()`; call `GetSystemInfo` and assert `capabilities.worker_heartbeats == true`; call `DescribeNamespace("default")` and assert `capabilities.worker_heartbeats == true`; register `EchoWorkflow` on a v0.4 `Worker`; start an `EchoWorkflow` execution with payload `{"msg": "hello"}`, await completion, assert the returned payload equals the input.
    - (Req 7.1.2, 7.1.3, 7.1.4, 7.1.5, 7.1.6, 7.2.2)
  - [ ] 16.2 Use `tokio::sync::Notify` for synchronisation; no explicit sleeps
    - All waits use `tokio::sync::Notify` or `tokio::time::timeout` over broadcast channels. No `tokio::time::sleep` or `std::thread::sleep` anywhere in the test per `tokeira/AGENTS.md` Rule 1.
    - Bound the heartbeat wait at 90 s and the total test runtime at ≤ 120 s on a developer laptop.
    - (Req 7.1.7)
  - [ ] 16.3 Gate the test with `#[ignore]` and a rationale comment
    - Add `#[ignore = "integration test; spawns tokeirad and a v0.4 SDK worker. See temporal-api-v1.62-sync."]` on the test function.
    - Verify the test does NOT run under `cargo test --workspace` by default, and DOES run under `cargo test --package tokeira-bench --test v0_4_integration -- --include-ignored`.
    - (Req 7.1.1)
  - [ ] 16.4 Adjust `apps/tokeira-bench/Cargo.toml` SDK pin if required by v1.62.11 compatibility
    - If the v0.4 SDK signatures referenced by `bench_worker.rs` or `bench_starter.rs` drift due to version pinning changes accompanying this spec, update the `temporalio-sdk` / `temporalio-client` pins to a minimum-diff version compatible with the v1.62.11 server surface. Do NOT introduce other SDK behavioural changes. Leave `bench_worker.rs` and `bench_starter.rs` source unchanged.
    - (Req 7.2.1, 7.2.3)

- [ ] 17. Documentation updates (§11 Migration Step 6)
  - [ ] 17.1 Update `README.md` and/or `CONTRIBUTING.md` with the supported API and SDK versions
    - Add or update a statement naming the supported Temporal API version (`v1.62.11`) and the SDK generation (`temporalio-sdk v0.4`). Replace any existing pin to `v1.43.0`.
    - (Req 8.2.1, 8.2.2, 8.2.3)
  - [ ] 17.2 Remove lingering references to the Commit_214895e shims in workspace docs and comments
    - Grep the workspace for references to the Commit_214895e rationale comments, the old `Vec<Vec<u8>> worker_heartbeat` shape, and any "interim shim" phrasing tied to `v1.43`. Update each to reference `temporal-api-v1.62-sync` or remove where the shim no longer exists.
    - Confirm `proto/upstream/` is free of hand-authored hunks via `git diff 214895e^..HEAD -- proto/upstream/` showing only the net effect of the resync.
    - (Req 3.5.1)

- [ ] 18. Final checkpoint — spec complete
  - Ensure all tests pass, ask the user if questions arise.
  - Validate the CI matrix from design.md §10 Testing Strategy: `cargo +nightly fmt --all --check`, `cargo lint`, `cargo check --workspace`, `cargo test --workspace`, `cargo test --package tokeira-bench --test v0_4_integration -- --include-ignored`, `rg "Tokeirad currently accepts heartbeats as a no-op" proto/upstream` returns zero hits, and `test "$(cat proto/UPSTREAM_VERSION)" = "v1.62.11"` passes.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP. Per the workflow, these include all unit, property, and integration test sub-tasks. The spec's correctness properties (P1–P7) are required by Feature 4.5 and Feature 5.1, but the act of writing property tests is the sub-task that can be deferred if an MVP cut is needed; the properties themselves remain invariants the implementation upholds.
- Each task references specific requirements in parentheses for traceability. Every requirement number from `requirements.md` Features 1–8 appears in at least one task's parenthetical reference.
- Checkpoints (tasks 3, 11, 14, 18) mark the handoff points between the six rollout steps from design.md §11 Migration and Rollout. Each step leaves `cargo build --workspace` green so intermediate commits are bisectable.
- Property tests live alongside their implementation parents (P1 under section 4 translator work, P4 under section 6 CountSchedules, P5 under section 7 UpdateTaskQueueConfig, P6 under section 12 deferred-stub blocks) or under section 13 for structural invariants that span multiple parents (P2, P3, P7).
- The Surface_Audit table in `design.md` §5 is the single source of truth for which RPC / field lands in which bucket. If the resynced proto tree (after task 2.1) reveals a row whose exact `Added In` version or field shape diverges from the audit, amend the table in the same commit as task 2.4 and carry the amendment forward.
- No task in this plan modifies `crates/tokeira-kernel/`. Any Impact Matrix escalation that would require kernel changes is deferred to the named follow-up spec per Req 5.2.
